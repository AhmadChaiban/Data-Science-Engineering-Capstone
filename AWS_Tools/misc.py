def user_feedback(message, task = 'main'):
    if task == 'main':
        print('***************************************')
        print(message)
        print('***************************************')

    elif task == 'sub':
        print('-----------------------')
        print(message)
        print('-----------------------')

def user_feedback_progress(message, task = 'main'):
    if task == 'main':
        print(message, end="\r")

    elif task == 'sub':
        print(message, end="\r")

