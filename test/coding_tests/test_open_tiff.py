import cv2


def main():
    path = "/space/test_data/flat/bf_00026.tif"

    img = cv2.imread(path, -1)
    # img = cv2.imread('s0087386.tif',-1)
    cv2.imshow('16bit TIFF', img)
    cv2.waitKey()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
