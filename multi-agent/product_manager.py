from langchain_core.messages import AIMessage
from utils.inference import call_model

def run(state):
    user_request = state["messages"][-1].content
    prompt = f"""You are a product manager. Convert the user’s request into clear, detailed product requirements. Be concise, and list out the main goals, features, and any user expectations.

User Request:
{user_request}

Respond with clear bullet points."""
    output = call_model(prompt)
    return {
        "messages": state["messages"] + [AIMessage(content=output)],
        "product_requirements": output
    }