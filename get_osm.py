import _osx_support
import os
import geopandas as gpd
import osmnx as ox
from math import radians, cos, sin, asin, sqrt
import pickle
import numpy as np
import pandas as pd
import time
import networkx as nx
import matplotlib.pyplot as plt
from multiprocessing import Process
import argparse
import socket
import requests
import json

# with open("list_csv1.json",'r') as load_f1:
#     load_dict1 = json.load(load_f1)
# #print(load_dict)
# list1=[]
# for i in load_dict1:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list1.append([float(j) for j in tmp])

# with open("list_csv11.json",'r') as load_f2:
#     load_dict2 = json.load(load_f2)
# #print(load_dict)
# list2=[]
# for i in load_dict2:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list2.append([float(j) for j in tmp])

# with open("list_csv12.json",'r') as load_f3:
#     load_dict3 = json.load(load_f3)
# #print(load_dict)
# list3=[]
# for i in load_dict3:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list3.append([float(j) for j in tmp])

# with open("list_csv13.json",'r') as load_f4:
#     load_dict4 = json.load(load_f4)
# #print(load_dict)
# list4=[]
# for i in load_dict4:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list4.append([float(j) for j in tmp])

# with open("list_csv14.json",'r') as load_f5:
#     load_dict5 = json.load(load_f5)
# #print(load_dict)
# list5=[]
# for i in load_dict5:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list5.append([float(j) for j in tmp])

# with open("list_csv15.json",'r') as load_f6:
#     load_dict6 = json.load(load_f6)
# #print(load_dict)
# list6=[]
# for i in load_dict6:
#     if not i.endswith(".csv"):
#         continue
#     if not i.startswith("nodes"):
#         continue
#     tmp=i[:-4].split("_")[1:]
#     #print(i)
#     list6.append([float(j) for j in tmp])

# sum_list=list1+list2+list3+list4+list5+list6



#函数get_osm即为目标函数，参数square表示一个经纬度块（[north,south,east,west]），
#tab表示经度和纬度分别被分成多少块，一共part**2块，注意part>=2
def get_osm(square,tab):
    #计算每一个小块的长和宽

    #list_file=os.listdir()

    #ferror = open("error_log.csv", "w")
    #ferror.close()

    part_lat = int(square[0]/0.2) + 1 - int(square[1]/0.2)
    part_lon = int(square[2]/0.2) - int(square[3]/0.2) + 1

    #list_nodes是一个列表，用来存储所有小块的点列表
    #list_edges是一个列表，用来存储所有小块的边列表
    list_nodes=[]
    list_edges=[]

    #search_info是用来获取每一小块数据的函数，参数为一个经纬度块，返回一个点列表和一个边列表
    def search_info(square0, square1, square2, square3):

        ferror = open("error_log.csv", "a")
        i=10
        while i>=1:
            try:
                #if (("nodes_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0, square1, square2, square3) in list_file) and ("edges_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0, square1, square2, square3) in list_file)):
                # if [square0,square1,square2,square3] in sum_list:
                #     break
                G1 = ox.graph_from_bbox(square0+0.01, square1-0.01, square2+0.01, square3-0.01,network_type='drive')
                G_projected1 = ox.project_graph(G1)
                #ox.plot_graph(G_projected)
                nodes1, edges1=ox.graph_to_gdfs(G_projected1)
                nodes1.to_csv("nodes_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0, square1, square2, square3))
                edges1.to_csv("edges_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0, square1, square2, square3))
                break
            except ox._errors.EmptyOverpassResponse:
                
                if i==1:       
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},ox._errors.EmptyOverpassResponse\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue
            except nx.exception.NetworkXPointlessConcept:
                if i==1: 
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},networkx.exception.NetworkXPointlessConcept\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue
            except (TimeoutError,socket.gaierror):
                i-=1
                time.sleep(2)
                continue
            except requests.exceptions.ConnectionError:
                if i==1: 
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},requests.exceptions.ConnectionError\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue
            except UnboundLocalError:
                if i==1: 
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},UnboundLocalError\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue    
            except ValueError:
                if i==1: 
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},ValueError\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue   
            except:
                if i==1: 
                    ferror.write("{:.4f}_{:.4f}_{:.4f}_{:.4f},unknown error\n".format(square0, square1, square2, square3))
                    break
                else:
                    i-=1
                    time.sleep(2)
                    continue   

        ferror.close()
            
        # G_projected1 = ox.project_graph(G1)
        # #ox.plot_graph(G_projected)
        # nodes1, edges1=ox.graph_to_gdfs(G_projected1)
        # nodes1.to_csv("nodes_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0-0.01, square1+0.01, square2-0.01, square3+0.01))
        # edges1.to_csv("edges_{:.4f}_{:.4f}_{:.4f}_{:.4f}.csv".format(square0-0.01, square1+0.01, square2-0.01, square3+0.01))

    #接下来将所有小块的经纬度块存入一个list里面
    list_square=[]
    for i in range(part_lat):
        for j in range(part_lon):
            list_square.append([ 0.2*(int(square[0]/0.2)+1)-i*tab, 0.2*(int(square[0]/0.2)+1)-(i+1)*tab, 0.2*(int(square[2]/0.2))-j*tab, 0.2*(int(square[2]/0.2))-(j+1)*tab])
    print(list_square)
    #下面开始获取每一小块的数据
#    for i in list_square:
#        search_info(i[0],i[1],i[2],i[3])
#         list_nodes.append(nodes)
#         list_edges.append(edges)
        
    list_p = []
    list_p_in_queue = []
    list_current_p = []
    list_p_id = []
    p_id = 0
    
    for sq in list_square:
        p = Process(target=search_info, args=(sq[0], sq[1], sq[2], sq[3]))
        list_p.append(p)
        list_p_in_queue.append(p)
        list_p_id.append(p_id)
        p_id += 1

    time1 = time.time()
    
    while len(list_p_in_queue) > 0 or len(list_current_p) > 0:
        while len(list_current_p) < 32 and len(list_p_in_queue) > 0:
            p = list_p_in_queue.pop()
            p_id = list_p_id.pop()
            print("now processing process {}/{}".format(p_id, len(list_p)))
            p.start()
            list_current_p.append(p)
        for p in list_current_p:
            if not p.is_alive():
                list_current_p.remove(p)
        time.sleep(10)
        time2 = time.time()
        cnt_remaining = len(list_p_in_queue)+len(list_current_p)
        cnt_finished = len(list_p)-cnt_remaining
        elasped_time = time2-time1
        remaining_time = elasped_time / cnt_finished * cnt_remaining if cnt_finished >0 else 999999
        print("finished {}/{}, elasped time: {:.2f}, estimated remaining time: {:.2f}".format(cnt_finished, len(list_p), elasped_time, remaining_time))
        
    for p in list_p:
        p.join()
    print("end join")
    
    # for file in os.listdir():
    #     if file.startswith("."):
    #         continue
    #     if file.endswith(".csv"):
    #         if file.startswith("nodes"):
    #             list_nodes.append(pd.read_csv(file, index_col="osmid"))
    #         if file.startswith("edges"):
    #             list_edges.append(pd.read_csv(file, index_col=["u", "v", "key"]))
    
    # print("sssssssssssssss", list_nodes[0])
    

    # #add_Osmidcol函数是在一个点列表后面加一列，用于存放它的osmid，便于之后画边时对于点的访问
    # def add_Osmidcol(nodes1):
    #     list_Osmidcol=[]
    #     for i in range(len(nodes1.lon)):
    #         #print(nodes1.index[i])
    #         list_Osmidcol.append(nodes1.index[i])
    #     nodes1['Osmidcol']=[x for x in list_Osmidcol]
    #     return nodes1

    # #下面将所有的点列表进行add_Osmidcol的操作
    # for i in range(part_lon*part_lat):
    #     list_nodes[i]=add_Osmidcol(list_nodes[i])
    # #此时的list_nodes里面的点列表已经全部加了osmid这一列
    
    # #add_uvcol函数是在一个边列表的后面加两列，用于存放对应两个端点的osmid
    # def add_uvcol(edges1):
    #     list_ucol=[]
    #     list_vcol=[]
    #     for i in range(len(edges1.length)):
    #         #print(nodes1.index[i])
    #         list_ucol.append(edges1.index[i][0])
    #         list_vcol.append(edges1.index[i][1])
    #     edges1['ucol']=[x for x in list_ucol]
    #     edges1['vcol']=[x for x in list_vcol]
    #     return edges1

    # #下面将所有的点列表进行add_uvcol的操作
    # for i in range(part_lon*part_lat):
    #     list_edges[i]=add_uvcol(list_edges[i])
    #此时的list_edges里面的点列表已经全部加了u,v这两列

    # #接下来将所有的点列表加到一起
    # sum_nodes=list_nodes[0].append(list_nodes[1])
    # for i in range(2,part_lon*part_lat):
    #     sum_nodes=sum_nodes.append(list_nodes[i])
    # sum_nodes=sum_nodes.drop_duplicates(['Osmidcol'])

    # #接下来将所有的边列表加到一起
    # sum_edges=list_edges[0].append(list_edges[1])
    # for i in range(2,part_lon*part_lat):
    #     sum_edges=sum_edges.append(list_edges[i])
    # sum_edges=sum_edges.drop_duplicates(['ucol','vcol'])

    # #下面将点列表和边列表分别存入一个csv文件
    # sum_nodes.to_csv("sum_nodes.csv")
    # sum_edges.to_csv("sum_edges.csv")
'''
    #sum_nodes_df是sum_nodes的副本
    sum_nodes_df=pd.DataFrame(sum_nodes)

    #下面给sum_nodes_df加一列从0开始的连续自然数，用于将sum_nodes_df的index从零开始排序
    list_index=[]
    for i in range(len(sum_nodes_df.lon)):
        #print(nodes1.index[i])
        list_index.append(i)
    sum_nodes_df['indexcol']=[x for x in list_index]
    sum_nodes_ini=sum_nodes_df.set_index('indexcol')
    #sum_nodes_ini的index是从零开始的
    
'''
'''
    #下面开始画图
    sum_nodes_lon_list=sum_nodes_ini.lon
    sum_nodes_lat_list=sum_nodes_ini.lat

    #画点
    plt.figure(figsize=(200,200))
    for i in range(len(sum_nodes_ini.lon)):
        plt.scatter(sum_nodes_lon_list[i],sum_nodes_lat_list[i],c='b',s=10)

    #画边
    for i in sum_edges.index:
        lon1=sum_nodes.lon[i[0]]
        lon2=sum_nodes.lon[i[1]]
        lat1=sum_nodes.lat[i[0]]
        lat2=sum_nodes.lat[i[1]]
        plt.plot([lon1,lon2],[lat1,lat2],c='r')
    plt.show()
'''

parser = argparse.ArgumentParser()
parser.add_argument('param1', type=float, help='display an float')
parser.add_argument('param2', type=float, help='display an float')
parser.add_argument('param3', type=float, help='display an float')
parser.add_argument('param4', type=float, help='display an float')
args = parser.parse_args()

#调用get_osm(square,part)
get_osm( [args.param1 , args.param2 , args.param3 , args.param4]  , 0.2)