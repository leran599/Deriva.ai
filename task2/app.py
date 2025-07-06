from flask import Flask, request, jsonify
from graph import build_graph
import os

graph = build_graph()
app = Flask(__name__)

@app.route("/sum_of_squares", methods=["POST"])
def sum_of_squares():
    body = request.get_json()
    length = body.get("length", 10)
    result = graph.invoke({"length": length})
    return jsonify(sum_of_squares=result["sum_of_squares"])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"Starting server on port {port}")
    app.run(port=port, host="0.0.0.0")
