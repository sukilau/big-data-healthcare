package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix


object NMF {

  /**
    * Run NMF clustering
    * @param V The original non-negative matrix
    * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
    * @param maxIterations The maximum number of iterations to perform
    * @param convergenceTol The maximum change in error at which convergence occurs.
    * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively
    */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {

    /**
      * TODO 1: Implement your code here
      * Initialize W, H randomly
      * Calculate the initial error (Euclidean distance between V and W * H)
      */
    var W = new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache)
    var H = BDM.rand[Double](k,V.numCols().toInt)
    var dist_prev = 0.0
    var dist_current = eucl_dist(V,W,H)
    var error = math.abs(dist_current - dist_prev)

    /**
      * TODO 2: Implement your code here
      * Iteratively update W, H in a parallel fashion until error falls below the tolerance value
      * The updating equations are,
      * H = H.* W^T^V ./ (W^T^W H)
      * W = W.* VH^T^ ./ (W H H^T^)
      */
    var i = 0
    V.rows.cache()

    while ((error > convergenceTol) && (i < maxIterations)) {
      var H_s = H * H.t
      W = dotProd(W, dotDiv(multiply(V, H.t), multiply(W, H_s)))
      W.rows.cache()
      var W_s = computeWTV(W, W)
      H = (H :* computeWTV(W, V)) :/ (W_s * H)

      dist_prev = dist_current
      dist_current = eucl_dist(V, W, H)
      error = math.abs(dist_current - dist_prev)
      i = i + 1

      // DEBUG
      // println("=====V=====")
      // V.rows.take(1).foreach(println)
      // println("=====W=====")
      // W.rows.take(1).foreach(println)
      // println("=====H=====")
      // println(H)
      // println("[MU DEBUG] iteration = " + i + ", error = " + error)
    }
    /** TODO: Remove the placeholder for return and replace with correct values */
    // (new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache), BDM.rand[Double](k, V.numCols().toInt))
    (W,H)
  }


  /**
    * RECOMMENDED: Implement the helper functions if you needed
    * Below are recommended helper functions for matrix manipulation
    * For the implementation of the first three helper functions (with a null return),
    * you can refer to dotProd and dotDiv whose implementation are provided
    */
  /**
    * Note:You can find some helper functions to convert vectors and matrices
    * from breeze library to mllib library and vice versa in package.scala
    */

  def eucl_dist(V: RowMatrix, W: RowMatrix,H: BDM[Double]): Double = {
    val A = multiply(W ,H)
    val X = Sub(V,A)
    dotProd(X,X).rows.map(a => a.toArray.sum).sum()/2
  }

  /** compute the mutiplication of a RowMatrix and a dense matrix */
  def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    X.multiply(fromBreeze(d))
  }

  /** get the dense matrix representation for a RowMatrix */
  def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    val A = new DenseMatrix(X.numCols().toInt,X.numRows().toInt,X.rows.collect.flatMap(x => x.toArray))
    A.t
  }

  /** matrix multiplication of W.t and V */
  def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {
    W.rows.zip(V.rows).map{
      case(w: Vector, v: Vector) => BDM.create(w.size, 1, w.toArray) * BDM.create(1, v.size, v.toArray)
    }.reduce(_+_)
  }

  /** sub of two RowMatrixes */
  def Sub(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :- toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }
}
