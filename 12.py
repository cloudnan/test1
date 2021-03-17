# --coding:UTF-8--
divice_id = []
system_id = []
device_value = []
system_value = []
with open('时区异常.txt','r') as fp:
    data_value = fp.readlines()
    for i in data_value:
        divice_id.append((i.split(' ')[1].split(':')[1]))
        =i.split(' ')[2].split(',')[0].split(':')[1]
print(divice_id)