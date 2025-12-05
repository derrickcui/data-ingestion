import email
import hashlib
from typing import Dict, Any, Optional, List
import asyncio
import aioimaplib
from email.header import decode_header
import trafilatura

from app.sources.base import BaseSource
from app.utility.log import logger


class EmailSourceFull(BaseSource):
    """极简全量 Email 抓取（不做增量、不持久化 state）"""

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        mailbox: str = "INBOX",
        port: int = 993,
        use_ssl: bool = True,
        concurrency: int = 8,
        user_metadata: Optional[Dict[str, Any]] = None,
        source_type: str = "email",
        max_emails: Optional[int] = None,          # 限制最多抓取多少封
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.mailbox = mailbox
        self.use_ssl = use_ssl
        self.concurrency = concurrency
        self.user_metadata = user_metadata or {}
        self.source_type = source_type
        self.max_emails = max_emails

    def read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self._async_read(context))
        except RuntimeError:  # 已经在事件循环中
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._async_read(context))

    async def _async_read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        # 初始化客户端时设置全局 timeout
        client = aioimaplib.IMAP4_SSL(self.host, self.port, timeout=30) if self.use_ssl else aioimaplib.IMAP4(self.host,
                                                                                                              self.port,
                                                                                                              timeout=30)

        try:
            await client.wait_hello_from_server()

            login_resp = await client.login(self.username, self.password)
            if login_resp.result != "OK":
                logger.error(f"Login failed: {login_resp.lines}")
                return results

            select_resp = await client.select(self.mailbox)
            if select_resp.result != "OK":
                logger.error(f"Select mailbox failed: {select_resp.lines}")
                return results

            # 修复：用普通 SEARCH 获取序列号
            search_resp = await client.search(None, "ALL")
            if search_resp.result != "OK":
                logger.error(f"SEARCH ALL failed: {search_resp.lines}")
                return results

            if not search_resp.lines or not search_resp.lines[0]:
                logger.info("Mailbox is empty.")
                return results

            seq_nums = search_resp.lines[0].decode('utf-8', errors='ignore').split()
            if not seq_nums:
                return results

            # 批量获取所有 UIDs（高效，一次请求）
            uids_resp = await client.fetch(",".join(seq_nums), "(UID)")
            if uids_resp.result != "OK":
                logger.error(f"Bulk UID fetch failed: {uids_resp.lines}")
                return results

            uids = []
            for line in uids_resp.lines:
                line_str = line.decode('utf-8', errors='ignore')
                if "UID " in line_str:
                    uid_match = line_str.split("UID ")[1].split(" ")[0].split(")")[0].strip()
                    if uid_match.isdigit():
                        uids.append(uid_match)

            uids = sorted(set(uids), key=int)  # 去重 + 按 UID 升序

            logger.info(f"Found {len(uids)} emails in total.")

            # 限制最大抓取数量
            if self.max_emails and self.max_emails > 0:
                uids = uids[:self.max_emails]
                logger.info(f"Limited to the first {self.max_emails} emails.")

            if not uids:
                return results

            semaphore = asyncio.Semaphore(self.concurrency)

            async def fetch_email(uid: str) -> List[Dict[str, Any]]:
                async with semaphore:
                    try:
                        # 使用 UID FETCH (BODY[]) - 这个是支持 UID 的
                        fetch_resp = await client.uid("FETCH", uid, "(BODY[])")
                        if fetch_resp.result != "OK":
                            logger.warning(f"UID {uid} fetch failed")
                            return []

                        # 正确拼接邮件原始内容
                        if len(fetch_resp.lines) < 3:
                            return []

                        raw_email = b"\r\n".join(fetch_resp.lines[1:-1])
                        if not raw_email:
                            return []

                        msg = email.message_from_bytes(raw_email)

                        subject = self._decode_header(msg.get("Subject", ""))
                        date_str = self._decode_header(msg.get("Date", ""))
                        sender = self._decode_header(msg.get("From", ""))

                        raw_text = ""
                        attachments = []

                        for part in msg.walk():
                            ctype = part.get_content_type()
                            disposition = part.get_content_disposition()

                            payload = part.get_payload(decode=True)
                            if not payload:
                                continue

                            if disposition == "attachment":
                                filename = self._decode_header(part.get_filename()) or "unknown.bin"
                                attachments.append({
                                    "file_name": filename,
                                    "binary": payload,
                                    "content_type": ctype,
                                })
                            elif ctype == "text/plain":
                                raw_text += payload.decode('utf-8', errors='ignore') + "\n"
                            elif ctype == "text/html":
                                html = payload.decode('utf-8', errors='ignore')
                                text = trafilatura.extract(html)
                                raw_text += (text or html) + "\n"

                        extracted_text = raw_text.strip()
                        score = len(extracted_text)

                        # 主文档 ID
                        doc_id = hashlib.sha256(
                            (subject + date_str + sender + uid).encode('utf-8')
                        ).hexdigest()[:16]

                        items: List[Dict[str, Any]] = []

                        # 正文文档
                        items.append({
                            "doc_id": doc_id,
                            "file_name": f"{subject or 'No Subject'}.txt",
                            "binary": extracted_text.encode("utf-8"),
                            "title": subject or "No Subject",
                            "raw_text": extracted_text,
                            "source_path": f"imap://{self.username}@{self.host}/{self.mailbox}/{uid}",
                            "source_type": self.source_type,
                            "user_metadata": {
                                "subject": subject,
                                "from": sender,
                                "date": date_str,
                                "uid": uid,
                                "content_score": score,
                                **self.user_metadata,
                            },
                        })

                        # 附件文档
                        for att in attachments:
                            att_id = hashlib.sha256(
                                (doc_id + att["file_name"]).encode('utf-8')
                            ).hexdigest()[:16]
                            items.append({
                                "doc_id": att_id,
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
                                    **self.user_metadata,
                                },
                            })

                        logger.debug(f"Fetched UID {uid} | Subject: {subject[:50]}")
                        return items

                    except Exception as e:
                        logger.error(f"Error fetching UID {uid}: {e}")
                        return []

            # 并行抓取
            tasks = [fetch_email(uid) for uid in uids]
            batches = await asyncio.gather(*tasks, return_exceptions=False)

            for batch in batches:
                results.extend(batch)

            # 按内容长度倒序
            results.sort(
                key=lambda x: x.get("user_metadata", {}).get("content_score", 0),
                reverse=True,
            )

            logger.info(f"Successfully fetched {len(results)} documents (including attachments).")

        finally:
            # 兼容新旧所有版本 aioimaplib 的安全退出
            try:
                if client and hasattr(client, "protocol") and client.protocol:
                    await asyncio.wait_for(client.logout(), timeout=10)
            except Exception as e:
                logger.debug(f"IMAP logout error (can be ignored): {e}")
            finally:
                try:
                    await client.stop()
                except:
                    pass

        return results

    @staticmethod
    def _decode_header(val: Optional[str]) -> str:
        if not val:
            return ""
        decoded = decode_header(val)
        parts = []
        for frag, enc in decoded:
            if isinstance(frag, bytes):
                enc = enc or "utf-8"
                try:
                    parts.append(frag.decode(enc, errors="ignore"))
                except:
                    parts.append(frag.decode("utf-8", errors="ignore"))
            else:
                parts.append(str(frag))
        return "".join(parts)