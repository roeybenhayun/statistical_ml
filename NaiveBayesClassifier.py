import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib.pyplot as plt
import math
from tempfile import TemporaryFile

class NaiveBayesClassifier:
    def __init__(self, distribution = None, data_dir_path=None ):
        self.__data_dir_path = data_dir_path 
        self.__data_file_list = ["test_0_img.mat", "test_0_label.mat", "test_1_img.mat", "test_1_label.mat", "train_0_img.mat", "train_0_label.mat", "train_1_img.mat", "train_1_label.mat"]
        self.__print_dims = False
        self.__plot_images = False
        self.__outfile = TemporaryFile()
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
                #print (mat_file)

    # Preperation:  load the data and reshape for easier matrix operations
    def setup(self):
        print ("\nsetup")
        print ("--------")
        self.load_data()
        self.reshape_data()

    # load the test and train data stored as mat file for digits 1 and 0
    def load_data(self):
        print ("Load test and train mat files")
        self.__test_0_image = scipy.io.loadmat (self.__data_path + "/test_0_img")
        self.__test_0_label = scipy.io.loadmat (self.__data_path + "/test_0_label")
        self.__test_1_image = scipy.io.loadmat (self.__data_path + "/test_1_img")
        self.__test_1_label = scipy.io.loadmat (self.__data_path + "/test_1_label")

        self.__train_0_image = scipy.io.loadmat (self.__data_path + "/train_0_img")
        self.__train_0_label = scipy.io.loadmat (self.__data_path + "/train_0_label")
        self.__train_1_image = scipy.io.loadmat (self.__data_path + "/train_1_img")
        self.__train_1_label = scipy.io.loadmat (self.__data_path + "/train_1_label")

        #for key, value in self.__train_0_lebel.items() :
        #    print (key, value)
        # np.savetxt("out/train_label_1", self.__train_1_label['target_label'],delimiter=",")
        # np.savetxt("out/test_label_1", self.__test_1_label['target_label'],delimiter=",")
        # np.savetxt("out/train_label_0", self.__train_0_label['target_label'],delimiter=",")
        # np.savetxt("out/test_label_0", self.__test_0_label['target_label'],delimiter=",")

        if (True == self.__print_dims):
            print ("\n******Imported data dims******")
            print ("__test_0_image :", self.__test_0_image['target_img'] .shape)
            print ("__test_1_image :", self.__test_1_image['target_img'] .shape)
            print ("__train_0_image :", self.__train_0_image['target_img'] .shape)
            print ("__train_1_image :", self.__train_1_image['target_img'] .shape)
            print ("\n")


    # reshape the images for easier matrix operations
    def reshape_data(self):
        print ("Reshape data")
        # shape the image data (rool the axis for easier computations). create function - format and prepare data
        # This will reshape the matrix from 28 x 28 x N to N x 28 x 28 (for computation ease) 
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
            print ("\n******Data dims*******")
            print ("__test_0_image :", self.__test_0_image['target_img'] .shape)
            print ("__test_1_image :", self.__test_1_image['target_img'] .shape)
            print ("__train_0_image :", self.__train_0_image['target_img'] .shape)
            print ("__train_1_image :", self.__train_1_image['target_img'] .shape)
            print ("\n")

    # extract the foolowing features from each 28x28 gray image:
    # Pixel intensity mean
    # Avarage of row pixel intensity variances 
    def extract_features(self):
        print ("\nextract features vectors")
        print ("--------------------------")
        print ("X1 - feature vector of mean brightness of every image")
        print ("X2 - feature vector of avarage of varaince of each row in image")
        # First feature X1 : pixel intensity mean value
        self.__test_0_mean_feature_x1 = self.__test_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__test_1_mean_feature_x1 = self.__test_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__train_0_mean_feature_x1 = self.__train_0_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)
        self.__train_1_mean_feature_x1 = self.__train_1_image['target_img'].mean(axis=(1,2),keepdims=True,dtype=np.float64)

        # Second feature X2 : avarage of varaiance of each row
        # Calc the variance
        self.__test_0_variance_feature_x2 = self.__test_0_image['target_img'].var(axis=2, dtype=np.float64)
        self.__test_1_variance_feature_x2 = self.__test_1_image['target_img'].var(axis=2, dtype=np.float64)
        self.__train_0_variance_feature_x2 = self.__train_0_image['target_img'].var(axis=2, ddof=0, dtype=np.float64)
        self.__train_1_variance_feature_x2 = self.__train_1_image['target_img'].var(axis=2, ddof=0, dtype=np.float64)

        # Avarage the variance
        self.__test_0_variance_feature_x2  = np.mean(self.__test_0_variance_feature_x2 ,axis=1,dtype=np.float64)
        self.__test_1_variance_feature_x2  = np.mean(self.__test_1_variance_feature_x2 ,axis=1, dtype=np.float64)
        self.__train_0_variance_feature_x2  = np.mean(self.__train_0_variance_feature_x2 ,axis=1, dtype=np.float64)
        self.__train_1_variance_feature_x2  = np.mean(self.__train_1_variance_feature_x2 ,axis=1,)

        # for debug. make sure matrix dims are correct.
        if (True == self.__print_dims):
            print ("\n******Mean dims*******")
            print ("__test_0_mean_feature_x1 :",self.__test_0_mean_feature_x1.shape)
            print ("__test_1_mean_feature_x1: ", self.__test_1_mean_feature_x1.shape)
            print ("__train_0_mean_feature_x1 :", self.__train_0_mean_feature_x1.shape)
            print ("__train_1_mean_feature_x1 : ",self.__train_1_mean_feature_x1.shape)
            
            print ("\n******Variance dims********")
            print ("__test_0_variance_feature_x2 :", self.__test_0_variance_feature_x2.shape)
            print ("__test_1_variance_feature_x2 : ", self.__test_1_variance_feature_x2.shape)
            print ("__train_0_variance_feature_x2 :", self.__train_0_variance_feature_x2.shape)
            print ("__train_1_variance_feature_x2: ", self.__train_1_variance_feature_x2.shape)
            print ("\n")


    # show gray scale image
    def show_image(self,image):
        print ("show_image")
        plt.imshow(image, cmap=plt.cm.gray)
        plt.show()

    # calc the prior probabilities of getting digit 0 and digits 1
    # in our case the auumption is that they are equal to 0.5
    def calc_prior_probabilities(self,digit):
        # Assumption : P(y=0)=P(y=1)=0.5
        if (digit == "1" or digit == "0"):
            return 0.5

    # Features are drwan from gaussian pdf. using the mean and variane (calculated from previous steps. the MLE estimator)
    # we can calculate the conditional probability 
    def calc_pdf(self,type, X,mean,var):
        if (type == 'Gaussian'):
            exponent = np.exp(-(np.power(X - mean, 2) / (2 * np.power(var, 2))))
            val = exponent / (np.sqrt(2 * math.pi * np.power(var, 2)))            
        elif(type == "Log"):
            val = ((np.log(1/(var * np.sqrt(2*math.pi)))) - (np.power(X-mean,2)/(2*np.power(var,2))))

        return val

    #This function gets the fetaure vector as input and return the MLE parameters: mean and variance
    def get_mle(self,X1,X2):
        meu_1_hat = np.mean(X1, dtype=np.float64)
        variance_1_hat = np.std(X1,ddof=0, dtype=np.float64)

        meu_2_hat = np.mean(X2, dtype=np.float64)
        variance_2_hat = np.std(X2, ddof=0, dtype=np.float64)
        return meu_1_hat,variance_1_hat, meu_2_hat, variance_2_hat

    # get the prior probabilities and MLE parameter estimators using the training data sets for digit 0 and digit 1
    # (we have 2 features ,2 digit and 2 MLE parameters so in totoal we have 8 parameters)
    def train(self):
        print ("\ntrain")
        print ("-------")
        # get prior probabilities
        self.__P_digit_equal_to_0 = self.calc_prior_probabilities("1")
        self.__P_digit_equal_to_1 = self.calc_prior_probabilities("0")
        # get MLE parameters for each feature and digit (we have 2 features ,2 digit and 2 MLE parameters so in totoal we have 8 parameters)
        self.__train0_meu_1_hat, self.__train0_variance_1_hat, self.__train0_meu_2_hat, self.__train0_variance_2_hat = self.get_mle(self.__train_0_mean_feature_x1, self.__train_0_variance_feature_x2)
        self.__train1_meu_1_hat, self.__train1_variance_1_hat, self.__train1_meu_2_hat, self.__train1_variance_2_hat = self.get_mle(self.__train_1_mean_feature_x1, self.__train_1_variance_feature_x2)
        
        print ("MLE parameters:")
        print ("mle sultion of mean for feature X1 for digit 0 = ", self.__train0_meu_1_hat)
        print ("mle sultion of variance for feature X1 for digit 0 = ",self.__train0_variance_1_hat)
        print ("mle sultion of mean for feature X2 for digit 0 = ", self.__train0_meu_2_hat)
        print ("mle sultion of variance for feature X2 for digit 0 = ", self.__train0_variance_2_hat)

        print ("\nmle sultion of mean for feature X1 for digit 1 = ",self.__train1_meu_1_hat)
        print ("mle sultion of variance for feature X1 for digit 1 = ", self.__train1_variance_1_hat)
        print ("mle sultion of mean for feature X2 for digit 1 = ", self.__train1_meu_2_hat)
        print ("mle sultion of variance for feature X2 for digit 1 = ", self.__train1_variance_2_hat)


    # test the model parameters. Assume iid, P(X1|y=0)*P(X2|y=0), P(X1|y=1)*P(X2|y=1)
    # to test digit 0 we will use the testing data and plug in the features to the gaussian pdf the to get the probability
    # Ne
    def test(self):
        print ("\ntest")
        print ("----")
        # (assume iid) 
        # P(X1|y=0)*P(X2|y=0) 
        # P(X1|y=1)*P(X2|y=1)
        # prior probabilities are the same for P(y=0) and P(y=1).Therefor they can be ignored
        
        calc_mode = "Gaussian"
        # test digit 0
        # P(X1|y=0)
        x1_pdf_digit_0 = self.calc_pdf(calc_mode,self.__test_0_mean_feature_x1,self.__train0_meu_1_hat,self.__train0_variance_1_hat)
        # P(X2|y=0) 
        x2_pdf_digit_0 = self.calc_pdf(calc_mode,self.__test_0_variance_feature_x2,self.__train0_meu_2_hat,self.__train0_variance_2_hat)

        # P(X1|y=1)
        x1_pdf_digit_0_ = self.calc_pdf(calc_mode,self.__test_0_mean_feature_x1,self.__train1_meu_1_hat,self.__train1_variance_1_hat)
        # P(X2|y=1)
        x2_pdf_digit_0_ = self.calc_pdf(calc_mode, self.__test_0_variance_feature_x2,self.__train1_meu_2_hat,self.__train1_variance_2_hat)

        x1_pdf_digit_0 = x1_pdf_digit_0.reshape(-1)
        x1_pdf_digit_0_ = x1_pdf_digit_0_.reshape(-1)

        if (calc_mode == 'Gaussian'):
            #P(X1|y=0)*P(X2|y=0)
            joint_p1 = np.multiply(x1_pdf_digit_0 , x2_pdf_digit_0)
            # P(X1|y=1)*P(X2|y=1)
            joint_p2 = np.multiply(x1_pdf_digit_0_ , x2_pdf_digit_0_)
        elif (calc_mode == "Log"):
            joint_p1 = np.add(x1_pdf_digit_0 , x2_pdf_digit_0)
            joint_p2 = np.add(x1_pdf_digit_0_ , x2_pdf_digit_0_)

        # 
        diff = np.greater(joint_p2,joint_p1)
        
        print ("***************************************************************")
        print ("** Total number of digit '0' test sample is ", self.__test_0_mean_feature_x1.size)
        print ("** Number of 1 labeled digits recognized as 0 is : " , np.sum(diff))
        print ("** Accuracy rete ", 100 * (self.__test_0_mean_feature_x1.size - np.sum(diff))/self.__test_0_mean_feature_x1.size, "%")
        print ("***************************************************************\n")

        # test digit 1
        x1_pdf_digit_1 = self.calc_pdf(calc_mode,self.__test_1_mean_feature_x1,self.__train0_meu_1_hat,self.__train0_variance_1_hat)
        # P(X2|y=0) 
        x2_pdf_digit_1 = self.calc_pdf(calc_mode,self.__test_1_variance_feature_x2,self.__train0_meu_2_hat,self.__train0_variance_2_hat)

        # P(X1|y=1)
        x1_pdf_digit_1_ = self.calc_pdf(calc_mode,self.__test_1_mean_feature_x1,self.__train1_meu_1_hat,self.__train1_variance_1_hat)
        # P(X2|y=1)
        x2_pdf_digit_1_ = self.calc_pdf(calc_mode,self.__test_1_variance_feature_x2,self.__train1_meu_2_hat,self.__train1_variance_2_hat)
        
        x1_pdf_digit_1 = x1_pdf_digit_1.reshape(-1)
        x1_pdf_digit_1_ = x1_pdf_digit_1_.reshape(-1)

        if (calc_mode == 'Gaussian'):
            joint_p1 = np.multiply(x1_pdf_digit_1 , x2_pdf_digit_1)
            joint_p2 = np.multiply(x1_pdf_digit_1_ , x2_pdf_digit_1_)
        elif (calc_mode == "Log"):
            joint_p1 = np.add(x1_pdf_digit_1 , x2_pdf_digit_1)
            joint_p2 = np.add(x1_pdf_digit_1_ , x2_pdf_digit_1_)


        diff = np.greater(joint_p1,joint_p2)

        print ("***************************************************************")
        print ("** Total number of digit '1' test sample is ", self.__test_0_mean_feature_x1.size)
        print ("** Number of 1 labeled digits recognized as 0 is : " , np.sum(diff))
        print ("** Accuracy rete ", 100 * (self.__test_0_mean_feature_x1.size - np.sum(diff))/self.__test_0_mean_feature_x1.size, "%")
        print ("***************************************************************\n")
    

def main():
    naiveBayesClassifier = NaiveBayesClassifier()
    naiveBayesClassifier.setup()
    naiveBayesClassifier.extract_features()
    naiveBayesClassifier.train()
    naiveBayesClassifier.test() 

if __name__ == "__main__":
    main()