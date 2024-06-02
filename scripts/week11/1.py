import random
import string

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

random_string = generate_random_string(20000)
print(random_string)