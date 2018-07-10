import os

if __name__ == '__main__':
    if os.getcwd() != '/ihme/centralcomp/auto_test_data':
        raise ValueError("Incorrect working dir")
