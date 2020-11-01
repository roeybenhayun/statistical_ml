import matplotlib.pyplot as plt
import numpy as np

from kMeansClustering import KMeansClustering

def main():

    objective_function_1 = []
    objective_function_2 = []

    # Datasets source
    # P. Fränti and S. Sieranoja
    # K-means properties on six clustering benchmark datasets
    # Applied Intelligence, 48 (12), 4743-4759, December 2018
    # https://doi.org/10.1007/s10489-018-1238-7
    # BibTex

    datasets = {
    "a1" : {
        "path" : "datasets/a_sets",
        "name" : "a3.txt",        
        "format" : "txt"
        },
    "a2" : {
        "path" : "datasets/a_sets",
        "name" : "a2.txt",        
        "format" : "txt"
        },
    "a3" : {
        "path" : "datasets/a_sets",
        "name" : "a1.txt",        
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

    dataset = "a1"
    
    kMeansClustering = KMeansClustering(datasets[dataset]["path"],datasets[dataset]["name"], datasets[dataset]["format"])
    
    start_cluster = 2
    number_of_clusters = 15
    number_of_runs = 1
    
    calc_sse = False

    k = np.arange(2,number_of_clusters+1)
    plot_objective_function = True

    
    # 1 for Random initialization (fast, might lead to a suboptimal solution)
    # 2 for kmeans+++ initialization (slow. better then random init)
    centroid_init_strategy = 1
    
    kMeansClustering.setup(centroid_init_strategy,number_of_clusters)
    kMeansClustering.compute(True)


    if (calc_sse == True):
        kMeansClustering.cleanup()

        if (plot_objective_function == True):
            plt.title('Elbow Graph')
            plt.xlabel('Number of Clusters K')
            plt.ylabel('Objective Function Value')

        for run in range(1, number_of_runs+1):
            for _k in range(start_cluster,number_of_clusters+1):
                kMeansClustering.setup(centroid_init_strategy,_k)
                objective_function_1.append(kMeansClustering.compute())

            if (plot_objective_function == True):
                label = "Strategy 1 - run" + str(run)
                plt.plot (k,objective_function_1, label = label)
                plt.legend()
                plt.show()

            objective_function_1 = []



if __name__ == "__main__":
    main()