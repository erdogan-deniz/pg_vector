def func():

    try:
        result = 1 / 0
    except ZeroDivisionError:
        print("Division by zero!")
    finally:
        print("Executing finally clause.")
    
    result