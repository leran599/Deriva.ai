import random
from typing import Annotated, TypedDict
from langgraph.graph import StateGraph
from langgraph.constants import Send
import operator


# 🧠 Define state structure
class State(TypedDict):
    length: int
    numbers: list[int]
    square: int
    squared_results: Annotated[list[int], operator.add]
    sum_of_squares: int

# 🎲 Generate random numbers
def generate_numbers(state: State) -> dict:
    return {"numbers": [random.randint(0, 99) for _ in range(state["length"])]}

# 🧮 Fan-out: return list of inputs
def fan_out(state: State) -> list[dict]:
    return [Send("square_number",{"square": n}) for n in state["numbers"]]

# ⛏️ Square a single number
def square_number(state: State) -> dict:
    return {"squared_results": [state["square"] ** 2]}

# ➕ Sum the squares
def reduce_squares(state: State) -> dict:
    return {"sum_of_squares": sum(state["squared_results"])}

# 🛠️ Graph builder
def build_graph():
    builder = StateGraph(State)

    builder.add_node("generator", generate_numbers)
    builder.add_node("fan_out", fan_out)
    builder.add_node("square_number", square_number)
    builder.add_node("reduce", reduce_squares)

    builder.set_entry_point("generator")
    builder.add_conditional_edges("generator", fan_out,["square_number"])
    builder.add_edge("square_number", "reduce")
    builder.set_finish_point("reduce")

    return builder.compile()
