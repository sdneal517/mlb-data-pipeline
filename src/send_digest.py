# send_digest.py

import pandas as pd
from common import get_conn, log
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import smtplib
from email.message import EmailMessage
import os


def build_pdf(df, pdf_path="watchability_digest.pdf"):
    """Build a PDF from the watchability_gold DataFrame."""
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph("MLB Watchability Digest", styles['Heading1']))
    elements.append(Spacer(1, 12))

    # Table
    data = [df.columns.tolist()] + df.values.tolist()
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightblue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
    ]))
    elements.append(table)

    doc.build(elements)
    return pdf_path


def send_email_with_pdf(pdf_path, recipients, subject="MLB Watchability Digest"):
    """Send the generated PDF to recipients via SMTP, with error logging."""
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("EMAIL_USER")  # set this in your env
    msg["To"] = ", ".join(recipients)
    msg.set_content("Attached is today's MLB Watchability Digest.")

    # Attach PDF
    with open(pdf_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename=os.path.basename(pdf_path),
        )

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(os.environ.get("EMAIL_USER"), os.environ.get("EMAIL_PASS"))
            smtp.send_message(msg)
        log.info(f"Email sent to {recipients}")
    except Exception as e:
        log.error(f"Failed to send email: {e}")
        raise


def main():
    # Load gold table
    with get_conn() as conn:
        df = pd.read_sql("SELECT * FROM watchability_gold", conn)

    # SAFEGUARD: handle empty dataframe
    if df.empty:
        log.warning("No rows in watchability_gold — skipping digest email")
        return

    pdf_path = build_pdf(df)
    send_email_with_pdf(pdf_path, recipients=["sdneal517@gmail.com"])


if __name__ == "__main__":
    main()
