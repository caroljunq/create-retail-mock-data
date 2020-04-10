import random
import time

def random_probabilities(min_value, max_value):
    """
    Generate random probabilities based range of interval
    PARAMETER
    MIN_VALUE: int
    MAX_VALUE: int
    PROBABILITIES: list of floats
    """
    delta = max_value - min_value + 1
    probabilities = [random.random() for _ in range(delta)] # random initial probabilities
    sum_probabilities = sum(probabilities)
    
    # https://stackoverflow.com/questions/2640053/getting-n-random-numbers-whose-sum-is-m
    # Just generate N random numbers, compute their sum, divide each one by the sum and multiply by M.
    for i in range(len(probabilities)):
        # generating probabilities equal to  1
        probabilities[i] = probabilities[i]/sum_probabilities
    
    return probabilities
    
# https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
def str_time_prop(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    # Return a random date between the passed interval
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)
