from langchain_core.messages import AIMessage
from utils.inference import call_model

def run(state):
    html = state["html_output"]
    iteration = state["iteration"]

    prompt = f"""You are a QA engineer. Review the following HTML code:

{html}

Evaluate the UI for:
1. Visual quality
2. Responsiveness
3. Code correctness
4. Functional completeness

If it is perfect, reply only with 'APPROVED'.
If not, list clear improvements required."""
    output = call_model(prompt)
    done = "APPROVED" in output.upper()
    return {
        "messages": state["messages"] + [AIMessage(content=output)],
        "qa_feedback": output,
        "iteration": iteration + 1,
        "done": done
    }