import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os



class NaiveBayesClassifier:
    def __init__(self, data_dir_path=None):
        self.__data_dir_path = data_dir_path 
        self.__data_file_list = ["test_0_img.mat", "test_0_label.mat", "test_1_img.mat", "test_1_label.mat", "train_0_img.mat", "train_0_label.mat", "train_1_img.mat", "train_1_label.mat"]

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
        self.__test_0_image = scipy.io.loadmat (self.__data_path + "/test_0_img")
        self.__test_0_lebel = scipy.io.loadmat (self.__data_path + "/test_0_label")
        self.__test_1_image = scipy.io.loadmat (self.__data_path + "/test_1_img")
        self.__test_1_label = scipy.io.loadmat (self.__data_path + "/test_1_label")

        self.__train_0_image = scipy.io.loadmat (self.__data_path + "/train_0_img")
        self.__train_0_lebel = scipy.io.loadmat (self.__data_path + "/train_0_label")
        self.__train_1_image = scipy.io.loadmat (self.__data_path + "/train_1_img")
        self.__train_1_label = scipy.io.loadmat (self.__data_path + "/train_1_label")

        # shape the image data (rool the axis)        
        self.__test_0_image['target_img'] = np.rollaxis(self.__test_0_image['target_img'],-1)
        self.__test_1_image['target_img'] = np.rollaxis(self.__test_1_image['target_img'],-1)
        self.__train_0_image['target_img'] = np.rollaxis(self.__train_0_image['target_img'],-1)
        self.__train_1_image['target_img'] = np.rollaxis(self.__train_1_image['target_img'],-1)

        print ("******Data dims*******")
        print (self.__train_0_image['target_img'] .shape)
        print (self.__test_1_image['target_img'] .shape)
        print (self.__train_0_image['target_img'] .shape)
        print (self.__train_1_image['target_img'] .shape)

        # calculate the mean of each Image,move to function, late create a dictionary to sotre the mean and variance        
        self.test_0_mean = self.__test_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.test_1_mean = self.__test_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.train_0_mean = self.__train_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.train_1_mean = self.__train_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)

        # make sure I have the right dims
        print ("******Mean dims*******")
        print (self.test_0_mean.shape)
        print (self.test_1_mean.shape)
        print (self.train_0_mean.shape)
        print (self.train_1_mean.shape)


        # calculate the variance of each column and avarage it. should be a function that gets a vector and returs and variance vector
        # need to think about the matrix dims
        self.__test_0_variance = self.__test_0_image['target_img'].var(axis=2, dtype=np.float64)
        self.__test_1_variance = self.__test_1_image['target_img'].var(axis=2, dtype=np.float64)
        self.__train_0_variance = self.__train_0_image['target_img'].var(axis=2, dtype=np.float64)
        self.__train_1_variance = self.__train_1_image['target_img'].var(axis=2, dtype=np.float64)

        # check dims. remove later
        print ("******Variance dims********")
        print (self.__test_0_variance.shape)
        print (self.__test_1_variance.shape)
        print (self.__train_0_variance.shape)
        print (self.__train_1_variance.shape)

        # avarage the variance
        self.__test_0_variance  = np.average(self.__test_0_variance ,axis=1)
        self.__test_1_variance  = np.average(self.__test_1_variance ,axis=1)
        self.__train_0_variance  = np.average(self.__train_0_variance ,axis=1)
        self.__train_1_variance  = np.average(self.__train_1_variance ,axis=1)

        # Check dims after avarage calc. remove later
        print ("******Avarage variance dims**********")
        print (self.__test_0_variance.shape)
        print (self.__test_1_variance.shape)
        print (self.__train_0_variance.shape)
        print (self.__train_1_variance.shape)


    def calc_mean(self,X):
        print ("Calculate mean")
        return X.mean(axis(1,2), keepdims=True, dtype=np.float64)

    def calc_avarage_row_varaince(self,X):
        print ("Calculate avarage variance of each row ")
        return np.average(X ,axis=1,dtype=np.float64)

    def train(self):
        

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