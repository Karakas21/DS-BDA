#!/usr/bin/python3
import random
import datetime
import getopt
import sys

def generate_ip_addr():
    return "%s.%s.%s.%s" % (random.randint(83,83), random.randint(167,167), random.randint(100,122), random.randint(0,255))

def generate_url():
    sites = ['https://www.example.edu/adjustment.aspx?branch=brake',
        'http://example.com/bells/aunt.aspx?bone=boat',
        'https://www.example.net/argument/box.php#book',
        'http://example.com/act',
        'http://www.example.com/',
        'http://www.example.org/blood',
        'https://www.example.com/',
        'https://behavior.example.edu/bell',
        'https://www.example.com/birds.html',
        'https://alarm.example.com/',
        'https://example.com/bridge',
        'http://www.example.com/airport.php?baby=beef#boundary',
        'https://www.example.com/?border=aunt&ball=bottle',
        'http://example.com/',
        'http://www.example.org/',
        'http://example.com/',
        'http://example.com/basket.htm',
        'https://www.example.com/?agreement=army&bike=airport',
        'https://example.com/apparel',
        'https://www.example.net/',
        'http://example.com/angle/appliance.html',
        'http://example.com/#bells',
        'http://example.com/?airport=approval',
        'http://animal.example.com/?bit=air&beginner=bridge',
        'http://believe.example.com/advertisement'
    ]
    return random.choice(sites)

def generate_datetime():
    dt = datetime.datetime(2020,
        random.randint(1, 9),
        random.randint(1, 22),
        random.randint(1, 23),
        random.randint(1, 59),
        random.randint(1, 59))
    return dt.strftime("%d/%b/%Y:%H:%M:%S")

def generate_bytes_count():
    return random.randint(50, 10200)

def generate_user_agent(agents):
    return random.choice(agents)

def generate_http_response():
    codes = [100,101,102,200,201,202,203,204,205,
             206,207,208,226,301,302,303,304,305,
             306,307,308,400,401,402,403,404,405,
             406,407,408,409,410,411,412,413,414,
             415,416,417,418,420,422,423,424,425,
             426,428,429,431,444,449,450,451,499,
             500,501,502,503,504,505,506,507,508,
             509,510,511,598,599]
    return random.choice(codes)

def generate_line(agents):
    return "%s - - [%s +0100] \"GET %s\" %s %s \"-\"%s" %(
        generate_ip_addr(),
        generate_datetime(),
        generate_url(),
        generate_http_response(),
        generate_bytes_count(),
        generate_user_agent(agents)
    )

def generate_error(line):
    isErr = random.choices([0,1], weights=[90,10], k=1)
    if not isErr[0]:
        return line
    else:
        result = ""
        arr = line.split(" ")
        first = True
        for i in arr:
            if not first:
                is_space = random.choices([0,1], weights=[50,50], k=1)
                if is_space[0]:
                    result += " "

            first = False
            err = random.choices([0,1], weights=[30,70], k=1)
            if err[0]:
                lst = list(i)
                random.shuffle(lst)
                i = ''.join(lst)
                i = i[0:random.randint(0, int(len(i)/2))]
                # i = "*" * int(len(i)-random.randint(0, int(len(i)/2)))
            result += i;
        return result
        
def help():
    print('args: --help -h, --size -s, --output -o, --count -c')

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:s:c:", ["help", "output=", "size=", "count="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err))
        help()
        sys.exit(2)
    if len(sys.argv[1:]) == 0:
        help()
        sys.exit(2)
    
    output = None
    size = 0
    splits_count = 0
    for o, a in opts:
        if o in ("-s", "--size"):
            size = int(a)
        elif o in ("-h", "--help"):
            sys.exit()
        elif o in ("-o", "--output"):
            output = a
        elif o in ("-c", "--count"):
            splits_count = int(a)
        else:
            print("unhandled option")
    if size == 0 or splits_count == 0 or output is None:
        print("params are not specified")
        sys.exit(2)
        
    agents = []
    with open("user_agents.txt", 'r') as fp:
        agents = fp.read().splitlines()

    limit_size = size*1024*1024
    print('Generator has started')
    for i in range(0, splits_count):
        sum_size = 0
        f = open(output+"_"+str(i)+".log", "w")
        while sum_size < limit_size:
            line = generate_line(agents)
            line = generate_error(line)
            sum_size += len(line)+1
            f.write(line+'\n')
        f.close()
        print("done %s/%s" % (i+1, splits_count))

if __name__ == "__main__":
    main()
    print("Generation ended")
