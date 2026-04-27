import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger
from core.settings import settings

class NotificationService:
    def send_email(self, subject: str, body: str, to_addresses: list[str] = None):
        """
        Send an email using the configured SMTP server.
        """
        if not settings.SMTP_USERNAME or not settings.SMTP_PASSWORD:
            logger.warning("SMTP credentials not set. Skipping email notification.")
            return

        sender_email = settings.EMAIL_FROM
        receiver_emails = to_addresses or ([e.strip() for e in str(settings.EMAIL_TO).split(",") if e.strip()] if settings.EMAIL_TO else [])

        if not receiver_emails:
            logger.warning("No recipient email addresses provided. Skipping email.")
            return

        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = ", ".join(receiver_emails)
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            logger.info(f"Connecting to SMTP server: {settings.SMTP_SERVER}:{settings.SMTP_PORT}")
            
            with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                server.sendmail(sender_email, receiver_emails, msg.as_string())
            
            logger.info(f"Email sent successfully to {receiver_emails}")

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            # We catch the exception because notification failure should not necessarily crash the pipeline,
            # but it is critical to log.
    def send_ingestion_report(self, client_name: str, batch_id: str, success_list: list, failure_list: list):
        """
        Sends a cumulative report of the ingestion batch.
        success_list: List of DatasetInfo objects
        failure_list: List of dicts {'file': str, 'reason': str}
        """
        total_files = len(success_list) + len(failure_list)
        subject = f"Ingestion Report: {client_name} - {batch_id} ({len(success_list)} Success, {len(failure_list)} Failed)"
        
        # Build HTML Body
        html_body = f"""
        <h2>Ingestion Batch Report</h2>
        <p><strong>Client:</strong> {client_name}</p>
        <p><strong>Batch ID:</strong> {batch_id}</p>
        <p><strong>Total Files Processed:</strong> {total_files}</p>
        <hr>
        """
        
        if success_list:
            html_body += "<h3>✅ Successful Ingestions</h3><ul>"
            for ds in success_list:
                html_body += f"<li>{ds.file_name} ({ds.file_size} bytes)</li>"
            html_body += "</ul>"
            
        if failure_list:
            html_body += "<h3>❌ Failed Ingestions</h3><ul>"
            for fail in failure_list:
                html_body += f"<li><strong>{fail['file']}</strong>: {fail['reason']}</li>"
            html_body += "</ul>"
            
        if not success_list and not failure_list:
            html_body += "<p>No files processed.</p>"

        self.send_email_html(subject, html_body)

    def send_email_html(self, subject: str, html_body: str, to_addresses: list[str] = None):
        """
        Send HTML email.
        """
        if not settings.SMTP_USERNAME or not settings.SMTP_PASSWORD:
            logger.warning("SMTP credentials not set. Skipping email.")
            return

        sender_email = settings.EMAIL_FROM
        receiver_emails = to_addresses or ([e.strip() for e in str(settings.EMAIL_TO).split(",") if e.strip()] if settings.EMAIL_TO else [])
        
        if not receiver_emails:
            return

        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = ", ".join(receiver_emails)
            msg["Subject"] = subject
            msg.attach(MIMEText(html_body, "html")) # HTML Content

            with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                server.sendmail(sender_email, receiver_emails, msg.as_string())
            
            logger.info(f"HTML Email sent to {receiver_emails}")

        except Exception as e:
            logger.error(f"Failed to send HTML email: {e}")
