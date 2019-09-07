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
        self.__print_dims = False
        self.__plot_images = False
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

    # Preperation:  load the data and reshape for easier matrix operations
    def setup(self):
        print ("setup")
        self.load_data()
        self.reshape_data()

    # load the test and train data stored as mat file for digits 1 and 0
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


    # reshape the images for easier matrix operations
    def reshape_data(self):
        print ("Reshape data")
        # shape the image data (rool the axis for easier computations). create function - format and prepare data    
        self.__test_0_image['target_img'] = np.rollaxis(self.__test_0_image['target_img'],-1)
        self.__test_1_image['target_img'] = np.rollaxis(self.__test_1_image['target_img'],-1)
        self.__train_0_image['target_img'] = np.rollaxis(self.__train_0_image['target_img'],-1)
        self.__train_1_image['target_img'] = np.rollaxis(self.__train_1_image['target_img'],-1)

        # plot an aribtery image from the test set. 
        if (True == self.__plot_images):
            self.show_image(self.__test_0_image['target_img'][55])
            self.show_image(self.__test_1_image['target_img'][99])


        # for debug. make sure matrix dims are correct.
        if (True == self.__print_dims):
            print ("******Data dims*******")
            print (self.__test_0_image['target_img'] .shape)
            print (self.__test_1_image['target_img'] .shape)
            print (self.__train_0_image['target_img'] .shape)
            print (self.__train_1_image['target_img'] .shape)

    # extract the foolowing features from each 28x28 gray image:
    # Pixel intensity mean
    # Avarage of row pixel intensity variances 
    def extract_features(self):
        print ("extract features")
        # First feature X1 : pixel intensity mean value
        self.__test_0_mean_feature_x1 = self.__test_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__test_1_mean_feature_x1 = self.__test_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__train_0_mean = self.__train_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__train_1_mean = self.__train_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)

        # Second feature X2 : avarage of varaiance of each row
        # Calc the variance
        self.__test_0_variance_feature_x2 = self.__test_0_image['target_img'].var(axis=2, dtype=np.float64)
        self.__test_1_variance_feature_x2 = self.__test_1_image['target_img'].var(axis=2, dtype=np.float64)
        self.__train_0_variance = self.__train_0_image['target_img'].var(axis=2, dtype=np.float64)
        self.__train_1_variance = self.__train_1_image['target_img'].var(axis=2, dtype=np.float64)

        # Avarage the variance
        self.__test_0_variance_feature_x2  = np.average(self.__test_0_variance_feature_x2 ,axis=1)
        self.__test_1_variance_feature_x2  = np.average(self.__test_1_variance_feature_x2 ,axis=1)
        self.__train_0_variance  = np.average(self.__train_0_variance ,axis=1)
        self.__train_1_variance  = np.average(self.__train_1_variance ,axis=1)

        # for debug. make sure matrix dims are correct.
        if (True == self.__print_dims):
            print ("******Mean dims*******")
            print (self.__test_0_mean_feature_x1.shape)
            print (self.__test_1_mean_feature_x1.shape)
            print (self.__train_0_mean.shape)
            print (self.__train_1_mean.shape)
            
            print ("******Variance dims********")
            print (self.__test_0_variance_feature_x2.shape)
            print (self.__test_1_variance_feature_x2.shape)
            print (self.__train_0_variance.shape)
            print (self.__train_1_variance.shape)

            print ("******Avarage variance dims**********")
            print (self.__test_0_variance_feature_x2.shape)
            print (self.__test_1_variance_feature_x2.shape)
            print (self.__train_0_variance.shape)
            print (self.__train_1_variance.shape)


    # show gray scale image
    def show_image(self,image):
        print ("show_image")
        plt.imshow(image, cmap=plt.cm.gray)
        plt.show()

    # calc the prior probabilities of getting digit 0 and digits 1
    # in our case the auumption is that they are equal to 0.5
    def calc_prior_probabilities(self,digit):
        print("calc_prior_probabilities")
        # Assumption : P(y=0)=P(y=1)=0.5
        if (digit == "1" or digit == "0"):
            return 0.5

    # Features are drwan from gaussian pdf. using the mean and variane (calculated from previous steps. the MLE estimator)
    # we can calculate the conditional probability 
    def calc_pdf(self,X,mean,var):
        exponent = np.exp(-(np.power(X - mean, 2) / (2 * np.power(var, 2))))
        val = 1 / (math.sqrt(2 * math.pi) * var) * exponent       
        return val

    #This function gets the fetaure vector as input and return the MLE parameters: mean and variance
    def get_mle(self,X1,X2):
        print ("get MLE")
        meu_1_hat = np.average(X1)
        variance_1_hat = np.var(X1) 
        meu_2_hat = np.average(X2)
        variance_2_hat = np.var(X2) 
        return meu_1_hat,variance_1_hat, meu_2_hat, variance_2_hat

    # get the prior probabilities and MLE parameter estimators using the training data sets for digit 0 and digit 1
    # (we have 2 features ,2 digit and 2 MLE parameters so in totoal we have 8 parameters)
    def train(self):
        print ("train") 
        # get prior probabilities
        self.__P_digit_equal_to_0 = self.calc_prior_probabilities("1")
        self.__P_digit_equal_to_1 = self.calc_prior_probabilities("0")
        # get MLE parameters for each feature and digit (we have 2 features ,2 digit and 2 MLE parameters so in totoal we have 8 parameters)
        self.__train0_meu_1_hat, self.__train0_variance_1_hat, self.__train0_meu_2_hat, self.__train0_variance_2_hat= self.get_mle(self.__train_0_mean, self.__train_0_variance)
        self.__train1_meu_1_hat, self.__train1_variance_1_hat, self.__train1_meu_2_hat, self.__train1_variance_2_hat = self.get_mle(self.__train_1_mean, self.__train_1_variance)
        
        print (self.__train0_meu_1_hat,self.__train0_variance_1_hat, self.__train0_meu_2_hat, self.__train0_variance_2_hat)
        print (self.__train1_meu_1_hat, self.__train1_variance_1_hat, self.__train1_meu_2_hat, self.__train1_variance_2_hat) 

    # test the model parameters. Assume iid, P(X1|y=0)*P(X2|y=0), P(X1|y=1)*P(X2|y=1)
    # to test digit 0 we will use the testing data and plug in the features to the gaussian pdf the to get the probability
    # Ne
    def test(self):
        print ("test")
        # (assume iid) 
        # P(X1|y=0)*P(X2|y=0) 
        # P(X1|y=1)*P(X2|y=1)
        # prior probabilities are the same for P(y=0) and P(y=1).Therefor they can be ignored
        
        # test digit 0
        # P(X1|y=0)
        x1_pdf_digit_0 = self.calc_pdf(self.__test_0_mean_feature_x1,self.__train0_meu_1_hat,self.__train0_variance_1_hat)
        # P(X2|y=0) 
        x2_pdf_digit_0 = self.calc_pdf(self.__test_0_variance_feature_x2,self.__train0_meu_2_hat,self.__train0_variance_2_hat)

        # P(X1|y=1)
        x1_pdf_digit_0_ = self.calc_pdf(self.__test_0_mean_feature_x1,self.__train1_meu_1_hat,self.__train1_variance_1_hat)
        # P(X2|y=1)
        x2_pdf_digit_0_ = self.calc_pdf(self.__test_0_variance_feature_x2,self.__train1_meu_2_hat,self.__train1_variance_2_hat)

        x1_pdf_digit_0 = x1_pdf_digit_0.reshape(-1)
        x1_pdf_digit_0_ = x1_pdf_digit_0_.reshape(-1)

        pp = np.multiply(x1_pdf_digit_0 , x2_pdf_digit_0)
        pp2 = np.multiply(x1_pdf_digit_0_ , x2_pdf_digit_0_)

        print (pp.shape, pp2.shape)

        # test digit 1
        x1_pdf_digit_1 = self.calc_pdf(self.__test_1_mean_feature_x1,self.__train0_meu_1_hat,self.__train0_variance_1_hat)
        # P(X2|y=0) 
        x2_pdf_digit_1 = self.calc_pdf(self.__test_1_variance_feature_x2,self.__train0_meu_2_hat,self.__train0_variance_2_hat)

        # P(X1|y=1)
        x1_pdf_digit_1_ = self.calc_pdf(self.__test_1_mean_feature_x1,self.__train1_meu_1_hat,self.__train1_variance_1_hat)
        # P(X2|y=1)
        x2_pdf_digit_1_ = self.calc_pdf(self.__test_1_variance_feature_x2,self.__train1_meu_2_hat,self.__train1_variance_2_hat)
        
        x1_pdf_digit_1 = x1_pdf_digit_1.reshape(-1)
        x1_pdf_digit_1_ = x1_pdf_digit_1_.reshape(-1)

        #if (True == self.__print_dims):
        if (True):
            print(x1_pdf_digit_0.shape, x2_pdf_digit_0.shape, x1_pdf_digit_0_.shape, x2_pdf_digit_0_.shape)
            print(x1_pdf_digit_1.shape, x2_pdf_digit_1.shape, x1_pdf_digit_1_.shape, x2_pdf_digit_1_.shape)
    

def main():
    print ("Setup")
    naiveBayesClassifier = NaiveBayesClassifier()
    naiveBayesClassifier.setup()
    naiveBayesClassifier.extract_features()
    naiveBayesClassifier.train()
    naiveBayesClassifier.test() 

if __name__ == "__main__":
    main()