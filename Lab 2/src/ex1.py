def get_weather(temp):
    if temp < 0:
        return "Freezing"
    elif temp < 10:
        return "Cold"
    elif temp < 20:
        return "Cool"
    elif temp < 30:
        return "Warm"
    else:
        return "Hot"
