from typing import TypedDict, List
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage
from agents import product_manager, project_manager, software_architect, software_engineer, qa

class InputState(TypedDict):
    messages: List[BaseMessage]
    product_requirements: str
    project_plan: str
    architecture: str
    html_output: str
    qa_feedback: str
    iteration: int
    done: bool

def run_pipeline_and_save(messages):
    state = {
        "messages": messages,
        "product_requirements": "",
        "project_plan": "",
        "architecture": "",
        "html_output": "",
        "qa_feedback": "",
        "iteration": 0,
        "done": False
    }

    workflow = StateGraph(InputState)
    workflow.add_node("product_manager", product_manager.run)
    workflow.add_node("project_manager", project_manager.run)
    workflow.add_node("software_architect", software_architect.run)
    workflow.add_node("software_engineer", software_engineer.run)
    workflow.add_node("qa", qa.run)

    workflow.add_edge("product_manager", "project_manager")
    workflow.add_edge("project_manager", "software_architect")
    workflow.add_edge("software_architect", "software_engineer")
    workflow.add_edge("software_engineer", "qa")
    workflow.add_conditional_edges("qa", lambda s: END if s["done"] or s["iteration"] >= 3 else "software_engineer")

    workflow.set_entry_point("product_manager")
    compiled = workflow.compile()
    return compiled.invoke(state)