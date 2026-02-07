

# simple Calculator

def add(x, y):
    return x + y

def subtract(x, y):
    return x - y

def multiply(x, y):
    return x * y

def divide(x, y):
    if y == 0:
        return "Cannot divide by Zero"
    return x / y

# Take input from a User
print("Select Operation: +  -  *  /")
operation = input("Enter Operation: ")
num1 = float(input("Enter First Number: "))
num2 = float(input("Enter Second number: "))

# Perform operation
if operation == '+':
    print("Result:", add(num1, num2))
elif operation == '-':
    print("Result:", subtract(num1, num2))
elif operation == '*':
    print("Result:", multiply(num1, num2))
elif operation == '/':
    print("Result:", divide(num1, num2))
else:
    print("Invalid operation")

     