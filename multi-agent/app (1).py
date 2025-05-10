import gradio as gr
import zipfile
import os
from langchain_core.messages import HumanMessage
from utils.langgraph_pipeline import run_pipeline_and_save

TEMP_DIR = "generated_output"
os.makedirs(TEMP_DIR, exist_ok=True)

def save_files(html: str, log: str):
    html_path = os.path.join(TEMP_DIR, "ui.html")
    log_path = os.path.join(TEMP_DIR, "agent_log.txt")
    zip_path = os.path.join(TEMP_DIR, "output_bundle.zip")

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(log)

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(html_path, arcname="ui.html")
        zipf.write(log_path, arcname="agent_log.txt")

    return html_path, log_path, zip_path

def process(user_input):
    messages = [HumanMessage(content=user_input)]
    final_state = run_pipeline_and_save(messages)

    html = final_state["html_output"]
    log = "\n\n".join([f"{msg.type.upper()}: {msg.content}" for msg in final_state["messages"]])

    html_file, log_file, zip_file = save_files(html, log)

    # Escape quotes for safe iframe embedding
    escaped_html = html.replace('"', "&quot;")
    preview = f"<iframe srcdoc='{escaped_html}' width='100%' height='600px' style='border:1px solid #ccc;'></iframe>"

    return preview, html, log, html_file, log_file, zip_file

with gr.Blocks() as demo:
    gr.Markdown("# 🤖 Multi-Agent UI Generator")
    user_prompt = gr.Textbox(label="Describe your UI", lines=4, value="A yoga retreat homepage with schedule and testimonials.")
    run_btn = gr.Button("Run Agents")

    gr.Markdown("### 🔍 Website Preview")
    preview_html = gr.HTML()

    gr.Markdown("### 🖥️ Generated HTML Code")
    html_output = gr.Textbox(lines=15, label="Final HTML")

    gr.Markdown("### 💬 Agent Dialogue")
    log_output = gr.Textbox(lines=15, label="Conversation Log")

    gr.Markdown("### 📥 Download Files")
    html_download = gr.File(label="Download HTML")
    log_download = gr.File(label="Download Agent Log")
    zip_download = gr.File(label="Download All (.zip)")

    run_btn.click(
        fn=process,
        inputs=[user_prompt],
        outputs=[preview_html, html_output, log_output, html_download, log_download, zip_download]
    )

if __name__ == "__main__":
    demo.launch() 