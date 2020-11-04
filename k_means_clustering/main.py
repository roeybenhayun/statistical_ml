import matplotlib.pyplot as plt
import numpy as np
from kMeansClustering import KMeansClustering
from 
def main():

    # Datasets source
    # P. Fränti and S. Sieranoja
    # K-means properties on six clustering benchmark datasets
    # Applied Intelligence, 48 (12), 4743-4759, December 2018
    # https://doi.org/10.1007/s10489-018-1238-7
    # BibTex

    datasets = {
    "a1" : {
        "path" : "datasets/a_sets",
        "name" : "a1.txt",        
        "format" : "txt"
        },
    "a2" : {
        "path" : "datasets/a_sets",
        "name" : "a2.txt",        
        "format" : "txt"
        },
    "a3" : {
        "path" : "datasets/a_sets",
        "name" : "a3.txt",        
        "format" : "txt"
        },
    "s1" : {
        "path" : "datasets/s_sets",
        "name" : "s1.txt",        
        "format" : "txt"
        },
    "s2" : {
        "path" : "datasets/s_sets",
        "name" : "s2.txt",        
        "format" : "txt"
        },
    "s3" : {
        "path" : "datasets/s_sets",
        "name" : "s3.txt",        
        "format" : "txt"
        },
    "s4" : {
        "path" : "datasets/s_sets",
        "name" : "s4.txt",        
        "format" : "txt"
        },
    "s4" : {
        "path" : "datasets/AllSamples",
        "name" : "AllSamples.mat",        
        "format" : "mat"
        },
    }

    dataset = "a2"
    plot_dataset = False

    kMeansClustering = KMeansClustering(datasets[dataset]["path"],datasets[dataset]["name"], datasets[dataset]["format"],plot_dataset)
    
    start_cluster = 2
    number_of_clusters = 15
    number_of_runs = 1
    
    one_shot = False
    calc_sse = True

    k = np.arange(2,number_of_clusters+1)
    plot_objective_function = True


    if (one_shot == True):
        kMeansClustering.setup(KMeansClustering.RANDOM_INIT,number_of_clusters)
        kMeansClustering.compute(False)
        kMeansClustering.get_sse()
        kMeansClustering.get_seperation()
        kMeansClustering.cleanup()


    # Finding the number of clusters using Elbow Method
    if (calc_sse == True):
        objective_function = []
        centroid_init_strategy = KMeansClustering.RANDOM_INIT
        kMeansClustering.cleanup()

        for run in range(0, number_of_runs):
            for _k in range(start_cluster,number_of_clusters+1):
                kMeansClustering.setup(centroid_init_strategy,_k)
                kMeansClustering.compute(False)
                objective_function.append(kMeansClustering.get_sse())
                kMeansClustering.cleanup()

            if (plot_objective_function == True):
                plt.title('Elbow Graph')
                plt.xlabel('Number of Clusters K')
                plt.ylabel('Objective Function Value')
                label = "Strategy 1 - run" + str(run)
                plt.plot (k,objective_function, c='b', label = label)
                plt.legend()
                plt.show()
            
            objective_function = []

if __name__ == "__main__":
    main()