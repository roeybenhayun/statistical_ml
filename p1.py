import scipy.io
import matplotlib.pyplot as plt
import os



class NaiveBayesClassifier:
    def __init__(self, data_dir_path=None):
        self.__data_dir_path = data_dir_path 
        self.__data_file_list = ["test_0_img.mat", "test_0_label.mat", "test_1_img.mat", "test_1_label.mat", "train_0_img.mat", "train_0_label.mat", "train_1_img.mat", "train_1_label.mat"]
        self.__print_data_dim = True

        if (data_dir_path is None):
            # assume data dir is in the current directory
            self.__data_path = os.path.dirname(os.path.realpath(__file__))
            # make sure required file are in the data_dir_path
            for mat_file in self.__data_file_list:
                if (False == os.path.isfile(mat_file)):
                    print ("Missing file : " + str(mat_file))
                print (mat_file)
        self.load_data()

    def load_data(self):
        print ("Load data")
        test_0_image = scipy.io.loadmat (self.__data_path + "/test_0_img")
        test_0_lebel = scipy.io.loadmat (self.__data_path + "/test_0_label")
        test_1_image = scipy.io.loadmat (self.__data_path + "/test_1_img")
        test_1_label = scipy.io.loadmat (self.__data_path + "/test_1_label")

        train_0_image = scipy.io.loadmat (self.__data_path + "/train_0_img")
        train_0_lebel = scipy.io.loadmat (self.__data_path + "/train_0_label")
        train_1_image = scipy.io.loadmat (self.__data_path + "/train_1_img")
        train_1_label = scipy.io.loadmat (self.__data_path + "/train_1_label")

        print (test_0_image['target_img'].mean())
        # target_lable
        # target_image
        # numpy.ndarray
        if (True == self.__print_data_dim):
            print ("test_0_image : " + str((test_0_image['target_img'].shape)))
            print ("test_0_lebel : " + str((test_0_lebel['target_label'].shape)))
            print ("test_1_image : " + str((test_1_image['target_img'].shape)))
            print ("test_1_label : " + str((test_1_label['target_label'].shape)))

            print ("train_0_image : " + str((train_0_image['target_img'].shape)))
            print ("train_0_lebel : " + str((train_0_lebel['target_label'].shape)))
            print ("train_1_image : " + str((train_1_image['target_img'].shape)))
            print ("train_1_label : " + str((train_1_label['target_label'].shape)))

    def train(self):
        #print (test_0_image['target_img'].mean())
        # calc and save the mean (feature vector 1)
        # calc and save the variance (feature vector 2)
        print ("train")
    def classify():
        print ("classify")
    

# calc first feature - mean 


# calc second feature - variance


# find probabilyt density function


def main():
    print ("Setup")
    naiveBayesClassifier = NaiveBayesClassifier()
        


if __name__ == "__main__":
    main()