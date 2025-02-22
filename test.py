def my_generator():
    print("Start")
    yield "Hello"  # ✅ Pauses execution and returns "Hello"
    print("End")
    yield "Helactually donelo" 

gen = my_generator()
print(next(gen))  # 🔹 Prints "Start" then returns "Hello"
print(next(gen))  # 🔹 Resumes after `yield`, prints "End"
