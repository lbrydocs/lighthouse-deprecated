def add_to_front(i, l):
    t = list(l)
    if i not in [r[0] for r in t]:
        t.pop()
        t.reverse()
        t.append(i)
        t.reverse()
    return t


def move_to_front(i, l):
    t = list(l)
    wrong_position = next((r for r in t[1:] if r[0] == i), False)
    if wrong_position:
        t.remove(wrong_position)
        t.reverse()
        t.append(i)
        t.reverse()
    return t
