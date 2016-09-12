# add item to front of list, dropping last item in the process
def add_to_front(i, l):
    t = list(l)
    if not isinstance(i, tuple):
        item = (i, 1000)
    else:
        item = i

    if item[0] not in [r[0] for r in t]:
        t.pop()
        t.reverse()
        t.append(item)
        t.reverse()
    return t


# if item is in list, move it to the front of the list
def move_to_front(i, l):
    t = list(l)
    wrong_position = next((r for r in t[1:] if r[0] == i), False)
    if wrong_position:
        t.remove(wrong_position)
        t.reverse()
        t.append(wrong_position)
        t.reverse()
    return t


def add_or_move_to_front(i, l):
    t = add_to_front(i, l)
    r = move_to_front(i, t)
    return r
