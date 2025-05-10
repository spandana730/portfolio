from langchain_core.messages import AIMessage
from utils.inference import call_model

def run(state):
    requirements = state["product_requirements"]
    prompt = f"""You are a project manager. Based on the following product requirements, create a task breakdown and milestone plan.

Requirements:
{requirements}

Respond with a structured plan with at least 3 milestones and 2-3 tasks each."""
    output = call_model(prompt)
    return {
        "messages": state["messages"] + [AIMessage(content=output)],
        "project_plan": output
    }