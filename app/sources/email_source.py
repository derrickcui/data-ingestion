import os
import email
import json
import hashlib
from typing import Dict, Any, Optional, List
import asyncio
import aioimaplib
from email.header import decode_header
import trafilatura
from app.sources.base import BaseSource
from app.utility.log import logger


class EmailSource(BaseSource):
    """
    异步 Email Source（全量+增量抓取版）
    - 外部接口 read() 保持同步，内部使用 _async_read() 异步抓取。
    - 支持并发抓取多封邮件。
    - 邮件正文抽取 + 内容评分。
    - 附件处理。
    - 已抓取邮件记录到本地状态文件。
    - 返回 List[Dict]，兼容 Pipeline。
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        mailbox: str = "INBOX",
        port: int = 993,
        use_ssl: bool = True,
        max_emails: int = 50,
        concurrency: int = 5,
        state_file: str = "email_source_state.json",
        reset_state: bool = False,
        user_metadata: Optional[Dict[str, Any]] = None,
        source_type: str = "email",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.mailbox = mailbox
        self.use_ssl = use_ssl
        self.max_emails = max_emails
        self.concurrency = concurrency
        self.state_file = state_file
        self.user_metadata = user_metadata if user_metadata else {}
        self.source_type = source_type

        if reset_state or not os.path.exists(self.state_file):
            self.seen_uids: set = set()
        else:
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    self.seen_uids = set(json.load(f))
            except Exception:
                self.seen_uids = set()

    # ----------------------
    # 外部同步接口
    # ----------------------
    def read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self._async_read(context))
        except RuntimeError as e:
            if "cannot run" in str(e):
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(self._async_read(context))
            raise e
        except Exception as e:
            logger.error(f"Failed to run async email read: {e}")
            return []

    # ----------------------
    # 内部异步逻辑
    # ----------------------
    async def _async_read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        client = aioimaplib.IMAP4_SSL(self.host, self.port) if self.use_ssl else aioimaplib.IMAP4(self.host, self.port)
        try:
            await client.wait_hello_from_server()
            login_resp = await client.login(self.username, self.password)
            if login_resp.result != "OK":
                logger.error(f"EmailSource login failed: {login_resp.lines}")
                return results

            select_resp = await client.select(self.mailbox)
            if select_resp.result != "OK":
                logger.error(f"EmailSource select mailbox {self.mailbox} failed: {select_resp.lines}")
                return results

            # ----------------------
            # 获取全量邮件 sequence numbers
            # ----------------------
            search_resp = await client.search("ALL")
            if search_resp.result != "OK":
                logger.error(f"EmailSource SEARCH ALL failed: {search_resp.lines}")
                return results

            seq_nums = search_resp.lines[0].decode().split()
            if not seq_nums:
                logger.info("No emails found in mailbox.")
                return results

            # ----------------------
            # 获取对应 UID 列表
            # ----------------------
            uid_list = []
            for seq in seq_nums:
                fetch_uid_resp = await client.fetch(seq, "(UID)")
                if fetch_uid_resp.result != "OK":
                    continue
                for line in fetch_uid_resp.lines:
                    try:
                        line_str = line.decode()
                        if "UID" in line_str:
                            uid_part = line_str.split("UID", 1)[1].strip()
                            uid = uid_part.strip(")").split()[0]
                            if uid.isdigit():
                                uid_list.append(uid)
                    except Exception:
                        continue

            # 去重 + 排序
            uid_list = sorted(list(set(uid_list)), key=int)

            # 增量筛选
            new_uids = [uid for uid in uid_list if uid not in self.seen_uids]
            if not new_uids:
                logger.info("No new emails to fetch.")
                await client.logout()
                return results

            # 最近 max_emails
            new_uids = new_uids[-self.max_emails:]

            semaphore = asyncio.Semaphore(self.concurrency)

            async def fetch_email(uid: str):
                async with semaphore:
                    try:
                        fetch_resp = await client.uid("FETCH", uid, "(RFC822)")
                        if fetch_resp.result != "OK":
                            logger.warning(f"Email fetch {uid} failed: {fetch_resp.lines}")
                            return []

                        raw_email = b"".join(fetch_resp.lines[1:-1])
                        msg = email.message_from_bytes(raw_email)

                        subject = self._decode_header(msg.get("Subject"))
                        date_str = self._decode_header(msg.get("Date"))
                        sender = self._decode_header(msg.get("From"))

                        raw_text = ""
                        attachments = []

                        for part in msg.walk():
                            content_type = part.get_content_type()
                            disposition = part.get_content_disposition()
                            payload = part.get_payload(decode=True)

                            if disposition == "attachment" and payload:
                                filename = self._decode_header(part.get_filename()) or "attachment.bin"
                                attachments.append({
                                    "file_name": filename,
                                    "binary": payload,
                                    "content_type": content_type
                                })
                            elif content_type in ["text/plain", "text/html"] and payload:
                                charsets_to_try = []
                                original_charset = part.get_content_charset()
                                if original_charset and original_charset.lower() not in ["unknown-8bit", "8bit", "binary", "default", "ascii", "none"]:
                                    charsets_to_try.append(original_charset)
                                charsets_to_try.extend(["utf-8", "latin-1"])

                                decoded = False
                                for cs in charsets_to_try:
                                    try:
                                        text = payload.decode(cs, errors="ignore")
                                        raw_text += text + "\n"
                                        decoded = True
                                        break
                                    except Exception:
                                        continue
                                if not decoded:
                                    logger.warning(f"Failed decoding email part for UID {uid}")

                        extracted_text = trafilatura.extract(raw_text) or ""
                        content_score = len(extracted_text.strip())
                        doc_id = hashlib.sha256((subject + date_str + sender).encode("utf-8")).hexdigest()[:16]

                        items: List[Dict[str, Any]] = []

                        # 邮件正文
                        items.append({
                            "doc_id": doc_id,
                            "file_name": f"{subject or 'email'}.txt",
                            "binary": extracted_text.encode("utf-8"),
                            "raw_text": extracted_text,
                            "source_path": f"imap://{self.username}@{self.host}/{self.mailbox}/{uid}",
                            "source_type": self.source_type,
                            "user_metadata": {
                                "subject": subject,
                                "from": sender,
                                "date": date_str,
                                "content_score": content_score,
                                **self.user_metadata
                            }
                        })

                        # 附件
                        for att in attachments:
                            att_doc_id = hashlib.sha256((doc_id + att["file_name"]).encode("utf-8")).hexdigest()[:16]
                            items.append({
                                "doc_id": att_doc_id,
                                "file_name": att["file_name"],
                                "binary": att["binary"],
                                "raw_text": None,
                                "source_path": f"imap://{self.username}@{self.host}/{self.mailbox}/{uid}/attachment/{att['file_name']}",
                                "source_type": f"{self.source_type}_attachment",
                                "user_metadata": {
                                    "subject": subject,
                                    "from": sender,
                                    "date": date_str,
                                    "content_type": att["content_type"],
                                    **self.user_metadata
                                }
                            })

                        self.seen_uids.add(uid)
                        return items
                    except Exception as e:
                        logger.error(f"Email fetch error {uid}: {e}")
                        return []

            all_results = await asyncio.gather(*[fetch_email(uid) for uid in new_uids])
            for batch in all_results:
                results.extend(batch)

            # 按正文评分排序
            results.sort(key=lambda x: x.get("user_metadata", {}).get("content_score", 0), reverse=True)

            # 更新状态文件
            try:
                with open(self.state_file, "w", encoding="utf-8") as f:
                    json.dump(list(self.seen_uids), f)
            except Exception as e:
                logger.warning(f"Failed to save email state: {e}")

        finally:
            await client.logout()

        return results

    # ----------------------
    # 强化 Header 解码
    # ----------------------
    @staticmethod
    def _decode_header(val: Optional[str]) -> str:
        if not val:
            return ""
        decoded_fragments = decode_header(val)
        decoded_str = ""
        for fragment, encoding in decoded_fragments:
            try:
                if isinstance(fragment, bytes):
                    charset_candidates = []
                    if encoding and isinstance(encoding, str) and encoding.lower() not in ["unknown-8bit", "8bit", "binary", "default", "none"]:
                        charset_candidates.append(encoding)
                    charset_candidates.extend(["utf-8", "latin-1"])
                    for cs in charset_candidates:
                        try:
                            decoded_str += fragment.decode(cs, errors="ignore")
                            break
                        except Exception:
                            continue
                else:
                    decoded_str += str(fragment)
            except Exception:
                continue
        return decoded_str
