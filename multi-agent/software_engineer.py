from langchain_core.messages import AIMessage
from utils.inference import call_model
import re

def run(state):
    architecture = state["architecture"]
    user_prompt = state["messages"][0].content

    prompt = f"""
You are a senior front-end developer. Generate a complete modern HTML page with embedded CSS that visually matches the user's request and UI architecture.

🧾 User Prompt:
"{user_prompt}"

📐 UI Architecture:
{architecture}

✅ Output Requirements:
- Start with <!DOCTYPE html> and end with </html>
- All CSS must be inside a <style> tag in the <head>
- Use semantic HTML5 tags: <nav>, <header>, <section>, <footer>, etc.

🎨 Visual Requirements:
- Navigation bar:
  - Fixed to top with background color
  - Horizontal <ul><li><a> layout centered using flexbox
  - Hover effect on links

- Hero section:
  - Full-screen height
  - Background image via Unsplash based on prompt theme
  - Semi-transparent dark overlay
  - Centered headline and subheadline with readable white text
  - A call-to-action button styled with .btn

- Section layout:
  - Use <section> with class .section
  - Alternate light/dark backgrounds (.section--light and .section--dark)
  - Limit content width with a .container class (max-width: 1200px)

- Buttons:
  - Use class .btn with hover effect, padding, rounded corners
  - Accent color (e.g. #007BFF) with hover shade

- Footer:
  - Dark background, centered text, padding

📱 Responsiveness:
- Use Flexbox/Grid
- Stack nav items vertically on small screens (<768px)
- Adjust padding/font-size

🚫 Do NOT:
- Include <link> or external stylesheets
- Use markdown or explanations
- Leave any element unstyled

Return only the final HTML code.
"""

    output = call_model(prompt)
    cleaned = re.sub(r"```(?:html|css)?", "", output).strip()

    matches = re.findall(r"<!DOCTYPE html>.*?</html>", cleaned, flags=re.DOTALL | re.IGNORECASE)
    if matches:
        final_html = matches[0]
    elif cleaned.lower().startswith("<!doctype"):
        final_html = cleaned
    else:
        final_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Generated UI</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background-color: #f8f9fa;
      margin: 0;
    }}
    header, footer {{
      padding: 1rem;
      background-color: #333;
      color: #fff;
      text-align: center;
    }}
  </style>
</head>
<body>
  {cleaned}
</body>
</html>"""

    return {
        "messages": state["messages"] + [AIMessage(content=final_html)],
        "html_output": final_html
    }