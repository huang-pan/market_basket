# nCr = n! / r! * (n - r)!
def n_length_combo(lst, n):
    if n == 0:
       return [[]]

    l =[]
    print('lst len(lst) n:', lst, len(lst), n)    
    for i in range(0, len(lst)):
        m = lst[i]
        remLst = lst[i + 1:]
        print('i m remLst', i, m, remLst)
        remainlst_combo = n_length_combo(remLst, n-1)
        print('remainlst_combo', remainlst_combo)
        for p in remainlst_combo:
            print('p', p)
            print('(m, *p)', (m, *p))
            l.append((m, *p))
            print('l', l)

    return l

arr = ['A', 'B']
arr = ['A', 'B', 'C']
arr = ['A', 'B', 'C', 'D'] # [['A', 'B'], ['A', 'C'], ['A', 'D'], ['B', 'C'], ['B', 'D'], ['C', 'D']]
print(n_length_combo(arr, 2))
