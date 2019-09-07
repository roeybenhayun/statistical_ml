import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib.pyplot as plt
import math


class NaiveBayesClassifier:
    def __init__(self, distribution = None, data_dir_path=None ):
        self.__data_dir_path = data_dir_path 
        self.__data_file_list = ["test_0_img.mat", "test_0_label.mat", "test_1_img.mat", "test_1_label.mat", "train_0_img.mat", "train_0_label.mat", "train_1_img.mat", "train_1_label.mat"]

        # the default distribution
        if (distribution is None):
            self.__distribution = 'Gaussian'

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
        print ("Load test and training data")
        self.__test_0_image = scipy.io.loadmat (self.__data_path + "/test_0_img")
        self.__test_0_lebel = scipy.io.loadmat (self.__data_path + "/test_0_label")
        self.__test_1_image = scipy.io.loadmat (self.__data_path + "/test_1_img")
        self.__test_1_label = scipy.io.loadmat (self.__data_path + "/test_1_label")

        self.__train_0_image = scipy.io.loadmat (self.__data_path + "/train_0_img")
        self.__train_0_lebel = scipy.io.loadmat (self.__data_path + "/train_0_label")
        self.__train_1_image = scipy.io.loadmat (self.__data_path + "/train_1_img")
        self.__train_1_label = scipy.io.loadmat (self.__data_path + "/train_1_label")

        # shape the image data (rool the axis). create function - format and prepare data    
        self.__test_0_image['target_img'] = np.rollaxis(self.__test_0_image['target_img'],-1)
        self.__test_1_image['target_img'] = np.rollaxis(self.__test_1_image['target_img'],-1)
        self.__train_0_image['target_img'] = np.rollaxis(self.__train_0_image['target_img'],-1)
        self.__train_1_image['target_img'] = np.rollaxis(self.__train_1_image['target_img'],-1)


        # show images
        #self.show_image(self.__test_0_image['target_img'][55])
        #self.show_image(self.__test_1_image['target_img'][99])

        print ("******Data dims*******")
        print (self.__test_0_image['target_img'] .shape)
        print (self.__test_1_image['target_img'] .shape)
        print (self.__train_0_image['target_img'] .shape)
        print (self.__train_1_image['target_img'] .shape)

        # calculate the mean of each Image,move to function, late create a dictionary to sotre the mean and variance 
        # Feature X1       
        self.__test_0_mean = self.__test_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__test_1_mean = self.__test_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        
        
        self.__train_0_mean = self.__train_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__train_1_mean = self.__train_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)

        
        # make sure I have the right dims
        print ("******Mean dims*******")
        print (self.__test_0_mean.shape)
        print (self.__test_1_mean.shape)
        print (self.__train_0_mean.shape)
        print (self.__train_1_mean.shape)


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

        # avarage the variance (feature X2)
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

    def show_image(self,image):
        plt.imshow(image, cmap=plt.cm.gray)
        plt.show()

    def calc_mean(self,X):
        print ("Calculate mean")
        return X.mean(axis(1,2), keepdims=True, dtype=np.float64)

    def calc_variance(self,X):
        print ("Calculate avarage variance of each row ")
        return np.average(X ,axis=1,dtype=np.float64)

    def calc_prior_probabilities(self,digit):
        # Assumption : P(y=0)=P(y=1)=0.5
        if (digit == "1" or digit == "0"):
            return 0.5

    def calc_pdf(self,X,mean,var):
        print (X.shape)
        #exponent = math.exp(-(math.pow(X - mean, 2) / (2 * math.pow(var, 2))))
        #val = 1 / (math.sqrt(2 * math.pi) * var) * exponent
        return 1

    #This function gets the fetaure vector as input and return the MLE parameters: mean and variance
    def get_mle(self,X):
        print ("get MLE")
        meu_hat = np.average(X)
        variance_hat = np.var(X) 
        return meu_hat,variance_hat


    def train(self):
        print ("train") 
        # 1. load data
        # 2. shape data
        # 3. calc mean
        # 4. calc variance
        # 5. calc probability
        P_digit_equal_to_0 = self.calc_prior_probabilities("1")
        P_digit_equal_to_1 = self.calc_prior_probabilities("0")
        mean_0_hat, variance_0_hat = self.get_mle(self.__test_0_mean)
        mean_1_hat, variance_1_hat = self.get_mle(self.__test_1_mean)
        print (mean_0_hat,variance_0_hat)
        print (mean_1_hat, variance_1_hat)
        # (assume iid) 
        # P(X1|y=0)*P(X2|y=0) 
        # P(X1|y=1)*P(X2|y=1)
        # prior probabilities are the same for P(y=0) and P(y=1).Therefor they can be ignored

        # P(X1|y=0)
        self.calc_pdf(self.__test_0_mean,mean_0_hat,variance_0_hat)
        # P(X2|y=0) 
        #self.calc_pdf(self.__test_0_variance,mean_0_hat,variance_0_hat)

        # P(X1|y=1)
        #self.calc_pdf(self.__test_1_mean,mean_1_hat,variance_1_hat)
        # P(X2|y=1)
        #self.calc_pdf(self.__test_1_variance,mean_0_hat,variance_0_hat)


    def test(self):
        print ("classify")
    

def main():
    print ("Setup")
    naiveBayesClassifier = NaiveBayesClassifier()
    naiveBayesClassifier.train()
    naiveBayesClassifier.test() 


if __name__ == "__main__":
    main()