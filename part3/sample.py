from dagster import op, job, In, Out

# --------------------
# Ops
# --------------------

@op(
    out=Out(int, description="Generates an integer"),
)
def generate_number():
    return 5


@op(
    out=Out(int, description="Generates another integer"),
)
def generate_another_number():
    return 10


@op(
    ins={"x": In(int), "y": In(int)},
    out=Out(int, description="Sum of two numbers"),
)
def add_numbers(x, y):
    return x + y


@op(
    ins={"value": In(int)},
    out=Out(int, description="Multiply by 2"),
)
def multiply_by_two(value):
    return value * 2


@op(
    ins={"a": In(int), "b": In(int)},
    out=Out(str, description="Final formatted string"),
)
def format_result(a, b):
    return f"The numbers are {a} and {b}"


# --------------------
# Job
# --------------------

@job
def math_job():
    num1 = generate_number()
    num2 = generate_another_number()
    
    summed = add_numbers(num1, num2)
    doubled = multiply_by_two(summed)
    
    format_result(a=doubled, b=num2)