from langchain_core.messages import AIMessage
from utils.inference import call_model

def run(state):
    plan = state["project_plan"]
    prompt = f"""You are a software architect. Based on the following project plan, describe the architecture for the front-end web app to be built. Include:

- Component structure
- Layout strategy
- Any libraries or standards
- HTML/CSS structuring notes

Project Plan:
{plan}"""
    output = call_model(prompt)
    return {
        "messages": state["messages"] + [AIMessage(content=output)],
        "architecture": output
    }