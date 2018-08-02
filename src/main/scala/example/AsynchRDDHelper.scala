package example

import org.apache.spark.rdd.RDD

import scala.collection.Iterator
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

/**
  * Iterator Adapter sur un iterator qui prend en charge l'execution asynchrone des traitements via
  *
  * @param iterator iterator des données de partition en entrée de chaque traitement
  * @param ec       un ExecutionContextExecutor
  * @tparam A type de donnée gérer par l'iterator
  */
class AsynchPartitionIterator[A](iterator: Iterator[A])(implicit ec: ExecutionContextExecutor) extends Iterator[A] {
  def mapAsync[B](f: A => B): Iterator[Future[B]] = new Iterator[Future[B]] {
    def hasNext = iterator.hasNext

    def next() = {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  def foreachAsync[U](f: A => U) {
    while (hasNext) {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): A = iterator.next()
}

/**
  * Singleton capable de prendre :
  *  - une fonction : prenant en entrée un AsynchPartitionIterator  et en sortie un tuple d'iterator de résultat Future et d'une fonction de terminaison
  *  - une partition de type Iterator:  permettant de construire un AsynchPartitionIterator sur les élément de la partition
  *
  * et d'executer de manière asynchrone le traitement ( la fonction ) de chaque élement de la partition
  *
  */
object AsynchRDDProcessor {

  import scala.concurrent.ExecutionContext.Implicits.global

  def processBatch[RESULT, ROW](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit), partition: Iterator[ROW]) = {
    //TODO à mettre en configuration
    val batchSize = 50
    //println("start pool")
    /*
    val executorWaiting = new ThreadPoolExecutor(batchSize, batchSize+1, 1, TimeUnit.MINUTES, new ArrayBlockingQueue[Runnable](batchSize * 2), new ThreadPoolExecutor.CallerRunsPolicy)
    val ecWaiting: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorWaiting)
    */
    val asynchIt: AsynchPartitionIterator[ROW] = new AsynchPartitionIterator[ROW](partition)
    val (futureIterator, close) = f(asynchIt)
    // Perform the sliding technique to queue only a set amount of records at a time
    val slidingIterator = futureIterator.sliding(batchSize - 1)
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map(futureBatch => {
      Await.result(futureBatch.head, 10 seconds)
    }) ++
      tailIterator.flatMap(lastBatch => {
        val r = lastBatch.map(future => Await.result(future, 10 seconds))
        close()
        r
      })
  }
}

/**
  * Singleton Helper fournissant l'implicit AsyncElementPartitionRDD : RDD permettant de traiter chaque element d'une partition de manière asynchrone
  *
  */
object AsynchRDDHelper {

  /**
    * Classe Implicite prenant un RDD fournissant
    *
    * @param rdd le RDD surlequel on souhaite ajouter le comportement de process asynchrone pour chaque element d'une partition
    * @tparam ROW type des données gérer par le RDD
    */
  implicit class AsyncElementPartitionRDD[ROW: ClassTag](rdd: RDD[ROW]) {


    def mapPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      rdd.mapPartitions(partition => {
        AsynchRDDProcessor.processBatch(f, partition)
      })
    }

    def foreachPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      rdd.foreachPartition(partition => {
        AsynchRDDProcessor.processBatch(f, partition)
      })
    }
  }

}
