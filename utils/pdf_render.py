# def render_report_like_pdf(data: dict):
#     print("\n=== Report View ===")
#     for k, v in data.items():
#         print(f"{k}: {v}")
#     print("\n(Report layout styled like PDF)")


# utils/pdf_render.py

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from datetime import datetime

def export_to_pdf(data: dict):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"report_{date_str}.pdf"
    c = canvas.Canvas(filename, pagesize=A4)
    width, height = A4
    y = height - 40
    c.setFont("Helvetica", 12)
    c.drawString(100, y, "Report Generated")
    y -= 20
    for k, v in data.items():
        c.drawString(100, y, f"{k}: {v}")
        y -= 20
    c.save()
    print(f"✅ PDF exported to {filename}")