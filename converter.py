def converter(code):
    # 4 bytes Mifare
    if len(code) == 8:
        b1 = code[0:2]
        b2 = code[2:4]
        b3 = code[4:6]
        b4 = code[6:8]
        pre_str = b4+b3+b2+b1
        came_code = str(int(pre_str, 16))
        if len(came_code) < 10:
            came_code = '00000000080' + came_code
        else:
            came_code = '0000000008' + came_code
        return came_code
    # 7 bytes
    elif len(code) == 14:
        byte_1 = str(int('8'+code[0:6], 16))
        if len(byte_1) == 9:
            byte_1_9 = byte_1[8:10]
            byte_1 = byte_1[0:8] + '0'+byte_1_9
        byte_2 = str(int(code[7:14], 16))
        if len(byte_2) == 8:
            byte_2_0 = byte_2[0:2]
            byte_2 = '0'+byte_2_0+byte_2[2:8]
        came_code = '0'+str(byte_1)+str(byte_2)
        return came_code


print(converter('042A80020C5380'))
print('00000000080639102709')
