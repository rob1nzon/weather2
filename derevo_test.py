from forecast import forecast
from error_check import check_error

if __name__ == '__main__':
    list_facs=[['delta', 'delta1', 'delta2', 'delta3', 'now'],
               ['temp', 'temp1', 'temp2', 'temp3', 'vl', 'vl1', 'vl2', 'vl3', 'delta', 'delta1', 'delta2', 'delta3', 'now'],
               ['temp', 'temp1', 'temp2', 'temp3', 'pa', 'pa1', 'pa2', 'pa3', 'vl', 'vl1', 'vl2', 'vl3', 'delta', 'delta1', 'delta2', 'delta3', 'now']]
    for fact in list_facs:
        forecast(factor=fact)
        print fact, check_error()[0]