import scipy.io
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib.pyplot as plt
import math
from tempfile import TemporaryFile

class DBSCAN:
    def __init__(self, dataset_path=None, dataset=None, format = None, plot_dataset=False):        
        self.__dataset_path = dataset_path
        self.__data_path = dataset_path
        self.__dataset = dataset 
        self.__data_file_list = [dataset_path + "/" + dataset]        
        self.__plot_dataset = plot_dataset
        self.__plot_centroids = False
        self.__save = False
        self.__log_enabled = False
        self.__unlabeled_data=[]
        self.__distances = []
        self.__clusters = []
        self.__eps = 2
        self.__min_pts = 2        
        self.__final_clusters = []        
        self.load_data(format)
    
    
    def cleanup(self):
        pass

    def setup(self,eps, min_pts):
        pass

    def load_data(self,format):
        """
        Load the input data (e.g. the samples to be clusters)
        Parameters
        ----------
        None

        Return
        ------
        None     
        """     
        area = np.pi*3   
        
        if (format == "txt"):
            self.__data = np.loadtxt(self.__data_path + "/" + self.__dataset)                        
            self.__unlabeled_data = self.__data
            self.__X1 = self.__unlabeled_data[:,[0]]
            self.__X2 = self.__unlabeled_data[:,[1]]  
            if (self.__plot_dataset == True):  
                plt.scatter(self.__X1, self.__X2,s=area,c='b',alpha=0.5)              
                plt.title(self.__dataset)
                plt.xlabel('X1')
                plt.ylabel('X2')       
                plt.show()


        if (self.__save == True):
            np.savetxt("data", self.__unlabeled_data,delimiter=",")
        