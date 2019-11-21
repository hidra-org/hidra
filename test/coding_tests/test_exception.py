from __future__ import print_function


def fun_a():
    print("I'm here")
    raise Exception


def main():
    fun_a()
    print("still here")


if __name__ == "__main__":
    main()
