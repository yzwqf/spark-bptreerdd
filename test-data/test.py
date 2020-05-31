import json
import random
import string

def genRandomString(slen=10):
    return ''.join(random.sample(string.ascii_letters + string.digits, slen))

def genRandomAge(start, end):
    return random.randint(start, end)


if __name__ =="__main__":
    number = 100
    template = {"id": None, "name":None, "age":None, "father":{"name":None, "age":None},"mother":{"name":None, "age":None}}
    file_name = "test.json"

    f = open(file_name, "w")
    for i in range(number):
        new_record = template
        new_record["id"]  = str(i)
        new_record["name"] = genRandomString()
        new_record["age"] = genRandomAge(0,90)
        new_record["father"]["name"] = genRandomString()
        new_record["father"]["age"] = new_record["age"] + genRandomAge(18, 40)
        new_record["mother"]["name"] = genRandomString()
        new_record["mother"]["age"] = new_record["age"] + genRandomAge(18, 40)
        f.write(json.dumps(new_record)+"\n")
    f.close()


