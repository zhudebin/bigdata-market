# bigdata-market

## mapreduce

<a href="http://www.aboutyun.com/thread-7304-1-1.html">引用</a>  

<img src="http://www.aboutyun.com/data/attachment/forum/201404/10/164714zxiexexxg05ihb14.jpg"/>  
> Shuffle的过程包括了Map端和Reduce端。  
> map 端  
<img src="http://www.aboutyun.com/data/attachment/forum/201404/10/164714t8ppnec488prnqqo.jpg"/>
> 1. Input Split分配给Map  
  2. Map进行计算，输出[key, value]形式的output  
  3. Map的输出结果缓存在内存里  
  4. 内存中进行Partition，默认是HashPartitioner(采用取模hash (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks)， 目的是将map的结果分给不同的reducer，有几个Partition，就有几个reducer，partition的数目可以在job启动时通过参数 “-Dmapreduc.job.reduces”设置(Hadoop 2.2.0), HashPartitioner可以让map的结果均匀的分给不同机器上的reducer，保证负载均衡。  
  5. 内存中在Partition结束后，会对每个Partition中的结果按照key进行排序。  
  6. 排序结束后，相同的key在一起了，如果设置过combiner，就合并数据，减少写入磁盘的记录数  
  7. 当内存中buffer(default 100M)达到阈值(default 80%)，就会把记录spill(即溢写)到磁盘中，优化Map时可以调大buffer的阈值，缓存更多的数据。  
  8. 当磁盘中的spill文件数目在3(min.num.spills.for.combine)个（包括）以上, map的新的output会再次运行combiner，而如果磁盘中spill file文件就1~2个，就没有必要调用combiner，因为combiner大多数情况和reducer是一样的逻辑，可以在reduer端再计算。  
  9. Map结束时会把spill出来的多个文件合并成一个，merge过程最多10（默认）个文件同时merge成一个文件，多余的文件分多次merge，merge过程是merge sort的算法。  
  10. Map端shuffle完毕，数据都有序的存放在磁盘里，等待reducer来拿取  
  
> Reducer端  
  1. shuffle and sort的过程不仅仅在map端，别忘了reducer端还没拿数据呢，reduce job当然不能开启。  
  2. Copy phase: reducer的后台进程(default 5个)到被Application Master (Hadoop 2.2), 或者之前的JobTracker 指定的机器上将map的output拷贝到本地，先拷贝到内存，内存满了就拷贝的磁盘。 
  3. Sort phase(Merge phase): Reduer采用merge sort，将来自各个map的data进行merge， merge成有序的更大的文件。  
  4. 如果设置过Combiner，merge过程可能会调用Combiner，调不调用要看在磁盘中产生的文件数目是否超过了设定的阈值。(这一点我还没有确认，但Combiner在Reducer端是可能调用。)  
  5. Reduce phase: reduce job开始，输入是shuffle sort过程merge产生的文件。 
