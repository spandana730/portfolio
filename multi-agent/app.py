import streamlit as st
import os
import time
import base64
from typing import Dict, List, TypedDict
from langgraph.graph import StateGraph, END
from huggingface_hub import InferenceClient

# Use Mistral model only
client = InferenceClient(
    model="mistralai/Mistral-7B-Instruct-v0.2",
    token=st.secrets["HF_TOKEN"]
)

class AgentState(TypedDict):
    messages: List[Dict[str, str]]
    design_specs: str
    html: str
    css: str
    feedback: str
    iteration: int
    done: bool
    timings: Dict[str, float]

DESIGNER_PROMPT = """You're a UI designer. Create design specs for:
{user_request}
Include:
1. Color palette (primary, secondary, accent)
2. Font choices
3. Layout structure
4. Component styles
Don't write code - just design guidance."""

ENGINEER_PROMPT = """Create a complete HTML page with embedded CSS for:
{design_specs}
Requirements:
1. Full HTML document with <!DOCTYPE>
2. CSS inside <style> tags in head
3. Mobile-responsive
4. Semantic HTML
5. Ready-to-use (will work when saved as .html)
Output JUST the complete HTML file content:"""

QA_PROMPT = """Review this website:
{html}
Check for:
1. Visual quality
2. Responsiveness
3. Functionality
Reply "APPROVED" if perfect, or suggest improvements."""

def time_agent(agent_func, state: AgentState, label: str):
    start = time.time()
    result = agent_func(state)
    duration = time.time() - start
    result["timings"] = state["timings"]
    result["timings"][label] = duration
    return result

def designer_agent(state: AgentState):
    specs = call_model(DESIGNER_PROMPT.format(user_request=state["messages"][-1]["content"]))
    return {"design_specs": specs, "messages": state["messages"] + [{"role": "designer", "content": specs}]}

def engineer_agent(state: AgentState):
    html = call_model(ENGINEER_PROMPT.format(design_specs=state["design_specs"]))
    if not html.strip().startswith("<!DOCTYPE"):
        html = f"""<!DOCTYPE html>
<html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'>
<title>Generated UI</title></head><body>{html}</body></html>"""
    return {"html": html, "messages": state["messages"] + [{"role": "software_engineer", "content": html}]}

def qa_agent(state: AgentState, max_iter: int):
    feedback = call_model(QA_PROMPT.format(html=state["html"]))
    done = "APPROVED" in feedback or state["iteration"] >= max_iter
    return {"feedback": feedback, "done": done, "iteration": state["iteration"] + 1,
            "messages": state["messages"] + [{"role": "qa", "content": feedback}]}

def call_model(prompt: str, max_retries=3) -> str:
    for attempt in range(max_retries):
        try:
            return client.text_generation(
                prompt,
                max_new_tokens=3000,
                temperature=0.3,
                return_full_text=False
            )
        except Exception as e:
            st.error(f"Model call failed (attempt {attempt+1}): {e}")
            time.sleep(2)
    return "<html><body><h1>Error generating UI</h1></body></html>"

def generate_ui(user_request: str, max_iter: int):
    state = {"messages": [{"role": "user", "content": user_request}], "design_specs": "", "html": "",
             "css": "", "feedback": "", "iteration": 0, "done": False, "timings": {}}

    workflow = StateGraph(AgentState)
    workflow.add_node("designer", lambda s: time_agent(designer_agent, s, "designer"))
    workflow.add_node("software_engineer", lambda s: time_agent(engineer_agent, s, "software_engineer"))
    workflow.add_node("qa", lambda s: time_agent(lambda x: qa_agent(x, max_iter), s, "qa"))
    workflow.add_edge("designer", "software_engineer")
    workflow.add_edge("software_engineer", "qa")
    workflow.add_conditional_edges("qa", lambda s: END if s["done"] else "software_engineer")
    workflow.set_entry_point("designer")
    app = workflow.compile()
    total_start = time.time()
    final_state = app.invoke(state)
    return final_state["html"], final_state, time.time() - total_start

def main():
    st.set_page_config(page_title="Multi-Agent Collaboration", layout="wide")
    st.title("🤝 Multi-Agent Collaboration")
    with st.sidebar:
        max_iter = st.slider("Max QA Iterations", 1, 5, 2)

    prompt = st.text_area("📝 Describe the UI you want:", "A coffee shop landing page with hero, menu, and contact form.", height=150)

    if st.button("🚀 Generate UI"):
        with st.spinner("Agents working..."):
            html, final_state, total_time = generate_ui(prompt, max_iter)
            st.success("✅ UI Generated Successfully!")
            st.components.v1.html(html, height=600, scrolling=True)

            st.subheader("📥 Download HTML")
            b64 = base64.b64encode(html.encode()).decode()
            st.markdown(f'<a href="data:file/html;base64,{b64}" download="ui.html">Download HTML</a>', unsafe_allow_html=True)

            # Communication History
            st.subheader("🧠 Agent Communication Log")
            history_text = ""
            for msg in final_state["messages"]:
                role = msg["role"].replace("_", " ").title()
                content = msg["content"]
                history_text += f"---\n{role}:\n{content}\n\n"
            st.text_area("Agent Dialogue", value=history_text, height=300)

            # Download Chat Log
            b64_hist = base64.b64encode(history_text.encode()).decode()
            st.markdown(
                f'<a href="data:file/txt;base64,{b64_hist}" download="agent_communication.txt" '
                'style="padding: 0.4em 1em; background: #4CAF50; color: white; border-radius: 0.3em; text-decoration: none;">'
                '📥 Download Communication Log</a>',
                unsafe_allow_html=True
            )
            st.subheader("📊 Performance")
            st.write(f"⏱️ Total Time: {total_time:.2f} seconds")
            st.write(f"🔁 Iterations: {final_state['iteration']}")
            for stage in ["designer", "software_engineer", "qa"]:
                st.write(f"🧩 {stage.title().replace('_', ' ')} Time: {final_state['timings'].get(stage, 0):.2f}s")

if __name__ == "__main__":
    main()