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
    """
    极简全量 Email 抓取版
    - 不使用 state
    - 不做增量过滤
    - 不做持久化
    - 输出格式完全保持与原增量版一致
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        mailbox: str = "INBOX",
        port: int = 993,
        use_ssl: bool = True,
        concurrency: int = 5,
        user_metadata: Optional[Dict[str, Any]] = None,
        source_type: str = "email",
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

    # ----------------------
    # 外部同步接口
    # ----------------------
    def read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self._async_read(context))
        except RuntimeError:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._async_read(context))

    # ----------------------
    # 内部异步逻辑
    # ----------------------
    async def _async_read(self, context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        client = aioimaplib.IMAP4_SSL(self.host, self.port) if self.use_ssl else aioimaplib.IMAP4(self.host, self.port)

        await client.wait_hello_from_server()
        login_resp = await client.login(self.username, self.password)
        if login_resp.result != "OK":
            logger.error(f"Email login failed: {login_resp.lines}")
            return results

        select_resp = await client.select(self.mailbox)
        if select_resp.result != "OK":
            logger.error(f"Email select failed: {select_resp.lines}")
            return results

        search_resp = await client.search("ALL")
        if search_resp.result != "OK":
            logger.error(f"Email SEARCH ALL failed: {search_resp.lines}")
            return results

        seq_nums = search_resp.lines[0].decode().split()
        if not seq_nums:
            return results

        # fetch all UIDs
        uids = []
        for seq in seq_nums:
            resp = await client.fetch(seq, "(UID)")
            if resp.result != "OK":
                continue
            for line in resp.lines:
                line_str = line.decode()
                if "UID" in line_str:
                    uid = line_str.split("UID")[1].split(")")[0].strip()
                    if uid.isdigit():
                        uids.append(uid)

        uids = sorted(set(uids), key=int)

        semaphore = asyncio.Semaphore(self.concurrency)

        async def fetch_email(uid: str):
            async with semaphore:
                try:
                    fetch_resp = await client.uid("FETCH", uid, "(RFC822)")
                    if fetch_resp.result != "OK":
                        return []

                    raw_email = b"".join(fetch_resp.lines[1:-1])
                    msg = email.message_from_bytes(raw_email)

                    subject = self._decode_header(msg.get("Subject"))
                    date_str = self._decode_header(msg.get("Date"))
                    sender = self._decode_header(msg.get("From"))

                    raw_text = ""
                    attachments = []

                    for part in msg.walk():
                        ctype = part.get_content_type()
                        disp = part.get_content_disposition()
                        payload = part.get_payload(decode=True)

                        if disp == "attachment" and payload:
                            filename = self._decode_header(part.get_filename()) or "attachment.bin"
                            attachments.append({
                                "file_name": filename,
                                "binary": payload,
                                "content_type": ctype
                            })
                        elif ctype in ["text/plain", "text/html"] and payload:
                            for cs in ["utf-8", "latin-1"]:
                                try:
                                    raw_text += payload.decode(cs, errors="ignore") + "\n"
                                    break
                                except:
                                    continue

                    extracted = trafilatura.extract(raw_text) or ""
                    score = len(extracted.strip())

                    doc_id = hashlib.sha256((subject + date_str + sender).encode()).hexdigest()[:16]

                    items = []

                    # 正文
                    items.append({
                        "doc_id": doc_id,
                        "file_name": f"{subject or 'email'}.txt",
                        "binary": extracted.encode("utf-8"),
                        "title": subject,
                        "raw_text": extracted,
                        "source_path": f"imap://{self.username}@{self.host}/{self.mailbox}/{uid}",
                        "source_type": self.source_type,
                        "user_metadata": {
                            "subject": subject,
                            "from": sender,
                            "create_": date_str,
                            "content_score": score,
                            **self.user_metadata
                        }
                    })

                    # 附件
                    for att in attachments:
                        a_id = hashlib.sha256((doc_id + att["file_name"]).encode()).hexdigest()[:16]
                        items.append({
                            "doc_id": a_id,
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

                    return items

                except Exception as e:
                    logger.error(f"Fetch email error {uid}: {e}")
                    return []

        all_batches = await asyncio.gather(*[fetch_email(uid) for uid in uids])
        for batch in all_batches:
            results.extend(batch)

        results.sort(key=lambda x: x.get("user_metadata", {}).get("content_score", 0), reverse=True)

        await client.logout()
        return results

    @staticmethod
    def _decode_header(val: Optional[str]) -> str:
        if not val:
            return ""
        decoded = decode_header(val)
        out = ""
        for frag, enc in decoded:
            if isinstance(frag, bytes):
                for cs in [enc, "utf-8", "latin-1"]:
                    if not cs:
                        continue
                    try:
                        out += frag.decode(cs, errors="ignore")
                        break
                    except:
                        continue
            else:
                out += frag
        return out
