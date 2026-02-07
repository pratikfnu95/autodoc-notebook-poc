

import json
import os
from datetime import datetime

TODO_FILE = "todos.json"

def load_todos():
    if not os.path.exists(TODO_FILE):
        return []
    with open(TODO_FILE, "r") as f:
        return json.load(f)

def save_todos(todos):
    with open(TODO_FILE, "w") as f:
        json.dump(todos, f, indent=2)

def list_todos(todos):
    if not todos:
        print("✅ No tasks.")
    else:
        for i, todo in enumerate(todos, 1):
            print(f"{i}. {todo['task']} - {todo['status']} ({todo['created']})")

def add_todo(todos):
    task = input("Enter task: ")
    todos.append({
        "task": task,
        "status": "Pending",
        "created": datetime.now().strftime("%Y-%m-%d %H:%M")
    })
    print("📝 Task added.")

def mark_done(todos):
    list_todos(todos)
    idx = int(input("Enter Task number to mark done: ")) - 1
    if 0 <= idx < len(todos):
        todos[idx]["status"] = "Done"
        print("✅ Task marked as done.")

def main():
    todos = load_todos()
    while True:
        print("\n1. List  2. Add  3. Done  4. Exit")
        choice = input("Choose: ")
        if choice == "1":
            list_todos(todos)
        elif choice == "2":
            add_todo(todos)
        elif choice == "3":
            mark_done(todos)
        elif choice == "4":
            save_todos(todos)
            break
        else:
            print("❌ Invalid option.")

if __name__ == "__main__":
    main()

     