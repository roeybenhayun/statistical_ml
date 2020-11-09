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
        self.__eps = eps
        self.__min_pts = min_pts
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
            self.__unlabeled_data = self.__data[:,0:2]
            self.__X1 = self.__unlabeled_data[:,[0]]
            self.__X2 = self.__unlabeled_data[:,[1]]  
            if (self.__plot_dataset == True):  
                plt.scatter(self.__X1, self.__X2,s=area,c='b',alpha=0.5)              
                plt.title(self.__dataset)
                plt.xlabel('X1')
                plt.ylabel('X2')       
                plt.show()
    
    def compute(self):
        clusters_list = []
        noise_list = []
        point_visited=np.zeros([self.__unlabeled_data.shape[0],1])
        for k in range(0,self.__unlabeled_data.shape[0]):            
            point_visited[k]=1
            point = self.__unlabeled_data[k]  
            indexes = self.region_quary(point)
            neighboor_pts = self.__unlabeled_data[indexes]
            if (neighboor_pts.shape[0] < self.__min_pts):
                print("point is noise")
                noise_list.append(point)
            else:                                
                c = []
                self.expend_cluster(point_visited,point,neighboor_pts,c,clusters_list)
                clusters_list.append(c)
        
        print("DBSCAN completed")
        


    def region_quary(self,point):
        l2_norm = np.linalg.norm(point - self.__unlabeled_data,keepdims = False, axis=1)                
        return np.where(l2_norm <= self.__eps)

    
    def expend_cluster(self,point_visited,point,neighboor_pts,c,clusters_list):        
        c.append(point)        
        for p_prime in range (0,neighboor_pts.shape[0]):
            val = neighboor_pts[p_prime]
            index = np.where(self.__unlabeled_data == val)
            i = index[0][0]
            if(point_visited[i] == 0):                
                point_visited[i] = 1
                indexes = self.region_quary(val)
                neighboor_pts_prime = self.__unlabeled_data[indexes]
                if (neighboor_pts_prime.shape[0] >= self.__min_pts):
                    neighboor_pts = np.vstack((neighboor_pts,neighboor_pts_prime))
            # iterate the cluster list
            found = False
            for i in clusters_list:
                for j in range (0,len(i)):
                    if(i[j][0] == val[0] and i[j][1] == val[1]):
                            found = True
                            break
                if (found == False):
                    c.append(val)
                else:
                    break
                

            
    