import com.datastax.driver.core.{Row, Session}

import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
  *
  * LICENCE
  *
  * (c) 2018 DataStax, Inc.
  * All rights reserved.
  * This software is made available to third parties by DataStax free of charge and subject to the following licence and conditions:
  * (i) it is provided independently of any existing contract(s) or relationships;
  * (ii) it is provided as-is and for educational purposes only;
  * (iii) it is provided without any warranty or support;
  * (iv) DataStax accepts no liability in relation to the use of this software whatsoever; and
  * (v) this licence shall continue until terminated, in DataStax’ sole discretion, on notice to the user.
  */
/*

  //REQUIREMENT:
  // Master election.
  // No more than 1 node can flag itself as master at the same time.
  // "Master priority": there is a priority for a node to be master: 1,2,3,4 => if master 1 dies, node 2 must be the next elected master (not 3 as long as 2 is alive)
  // Short election time (first test at 5 sec)
  // Possibility to force a node to become master.

  //NOTES:
  //Exception type not properly handled
  //Use prepared statement everywhere
  //request at CL=QUORUM
  //LTW with CL = SERIALIZABLE
  //TODO speculative retry? We need to check the response time of a LWT with iter-DC latencies to make sure the 500ms timeout is also fine.
    If not, we'll have to create a separate profile for LWT queries with different speculative retry config.

  //TABLES:
  CREATE TABLE masters (
      type text PRIMARY KEY,
      active boolean,
      node_id int,
      time timestamp
  ) WITH default_time_to_live = 5

  CREATE TABLE node_status (
      node_id int PRIMARY KEY,
      state text
  ) WITH default_time_to_live = 5
  */

object MasterElection {

  val TABLE_TTL = 5000
  val DRIVER_TIMEOUT = 2000 //We remove the driver TTL to avoid flapping master during C* timeout (when the connection is lost)
  val MARGIN = 250 //Safety to avoid keep the lease a little less than the table TTL to avoid having 2 masters at the same time
  val CURRENT_NODE_ID = 1
  val MASTER_TYPE = "TYPE1"

  def debug(msg: String) = ???
  def info(msg: String) = ???
  def warn(msg: String) = ???

  val session: Session = ???
  @volatile var masterId : Int = null

  //Start internal loop.
  internalLeaseCheck()

  /**
    * Find the master. Entry point, will be called every second in normal run to make sure the master still owns its lock.
    * Doesn't require SERIALIZABLE select.
    */
  @tailrec
  def findMaster(): Unit = {
    try{
      val result = session.execute(s"select node_id, ttl(node_id) as ttl from masters where type='$MASTER_TYPE' ").one()
      if(result == null){
        debug(s"No mater available for $MASTER_TYPE, try to get the lease")
        getLease()
      } else {
        updateMasterId(result)
      }
    } catch {
      case e: Exception => {
        warn(s"Couldn't find the $MASTER_TYPE master (can't get reach C* Quorum). Assuming old maste")
        //Something is wrong, just retry.
        Thread.sleep(1000)
        findMaster()
      }
    }
  }

  /**
    * Force the current node as master.
    */
  def forceCurrentNodeAsMaster(): Unit = {
    if(masterId == CURRENT_NODE_ID){
      warn("the current node is already the master.")
    } else {
      try {
        //Update with LWT to avoid potential concurrent update conflict
        val result = session.execute(s"update masters set active=false, node_id=$CURRENT_NODE_ID, time=dateof(now()) where type='$MASTER_TYPE' IF node_id!=$CURRENT_NODE_ID")
        if(result.wasApplied()){
          info("current node has the lease with active=false. Will refresh the lease once with active=false: " +
            "We need to be sure that the previous master stop being master before being active")
          refreshLease(0, false)
        } else {
          warn("current node already has the lease. Something is wrong. You should wait & retry")
        }
      }catch {
        case e: Exception => {
          warn("Can't force the current node as master (timeout might have worked). You should wait & retry")
        }
      }
    }
  }

  /**
    * No entry in the table. Try to get a lease depending of the nodes priority.
    */
  def getLease(): Unit = {
    val nodesWithHigherPriority = getNodesIdWithHigherPriority()
    info("Master is down. Finding nodes UP with higher priority...")
    if (nodesWithHigherPriority.isEmpty) {
      info("The current node has the higher priority. Getting lease")
      acquiereLease()
    } else {
      try {
        val nodesUP = session.execute(s"select node_id from node_status where node_id in(${nodesWithHigherPriority.mkString}) ").all().toList
        //TODO add here logic to put TYPE1 + TYPE2 master on the same node only if it's the only UP
        if (nodesUP.size() > 0) {
          info(s"Nodes ${nodesUP.map(_.getInt("node_id")).mkString} with higher priority are UP. Won't try to get the lease as long as they are UP")
          Thread.sleep(1000)
          findMaster()
        } else {
          info(s"No nodes with higher priority UP. Getting lease.")
          acquiereLease()
        }
      } catch {
        case e: Exception => {
          warn("Couldn't find the nodes UP (can't get reach C* Quorum). Will retry in 1 sec")
          Thread.sleep(1000)
          findMaster()
        }
      }
    }
  }

  var masterUntil = 0L
  //just a dumb example, use the automate instead
  def internalLeaseCheck() = {
    new Thread {
      override def run: Unit = {
        while (true){
          if(masterId == CURRENT_NODE_ID && System.currentTimeMillis() > masterUntil) {
            masterId == null
            warn("couldn't renew the lock before expiration. C* lock can be lost. Stop being master.  ")
          }
          Thread.sleep(10)
        }
      }
    }.run()
  }

  def refreshLease(retry: Int = 0, active: Boolean = true): Unit = {
    val sleepTime = TABLE_TTL - DRIVER_TIMEOUT - MARGIN
    debug(s"Refreshing lease in TABLE_TTL - DRIVER_TIMEOUT - MARGIN = $TABLE_TTL - $DRIVER_TIMEOUT - $MARGIN = $sleepTime ms...")
    Thread.sleep(sleepTime)
    try {
      val t = System.currentTimeMillis()
      debug(s"Refreshing lease now with active=$active.")
      val update = session.execute(s"update masters set active=$active, node_id=$CURRENT_NODE_ID, time=dateof(now()) where type='$MASTER_TYPE' if node_id = $CURRENT_NODE_ID")
      if(update.wasApplied()){
        renewInternalTimer(t)
        refreshLease(0, true)
      } else {
        warn(s"Couldn't renew the lease: node ${update.one().getInt("node_id")} has acquired the lease in between. Stop being master.")
        updateMasterId(update.one())
      }
    } catch {
      case e: Exception => {
        if(retry == 0){
          warn(s"Couldn't refresh the lease: ${e.getMessage}. First attempt. Retrying in 100ms...")
          //Sleep 100ms to give some room to C*
          Thread.sleep(100)
          refreshLease(1, active)
        } else {
          warn("Couldn't refresh the lease after 1 retry. Won't retry anymore, getting new master IP in 1sec")
          //Sleep 1sec to give some room to C*
          Thread.sleep(1000)
          findMaster()
        }
      }
    }
  }

  private def renewInternalTimer(t: Long) = {
    //We can't be master for more than the table TTL to avoid having 2 masters at the same time
    val until = t + TABLE_TTL - MARGIN
    debug(s"lease renewed. The current node will only be master until t + TABLE_TTL - MARGIN = $t + $TABLE_TTL - $MARGIN = $until (old timer=$masterUntil)")
    //Each refresh of the lease must reset the master id to itself (it could have been set to null in between)
    masterId = CURRENT_NODE_ID
    masterUntil = until
  }

  def acquiereLease() = {
    try {
      //Must get the current time BEFORE the query execution
      val t = System.currentTimeMillis()
      //Do not use now() but a var. the time is just here to debug.
      val insert = session.execute(s"insert into masters (type, active, node_id, time) values ('$MASTER_TYPE', true, 1, dateof(now())) IF NOT EXISTS")
      if(insert.wasApplied()) {
        renewInternalTimer(t)
        info(s"Current node has been elected as $MASTER_TYPE master")
        refreshLease(0, true)
      } else {
        val row = insert.one()
        warn(s"Couldn't get the lease, the node ${row.getInt("node_id")} has been elected as master in between, updating local master Id (current id =$masterId)")
        updateMasterId(row)
      }
    } catch {
      case e: Exception => {
        warn("Couldn't get the lease (can't get reach C* Quorum). Will retry in 1 sec")
        Thread.sleep(1000)
        findMaster()
      }
    }
  }

  private def updateMasterId(result: Row) = {
    if (!result.getBool("active")) {
      warn(s"Master isn't active. Should only happen when the master switch. $MASTER_TYPE master is temporary unknown.")
      masterId = null
    } else {
      val id = result.getInt("node_id")
      if (id != masterId) {
        warn(s"Master has changed from ${masterId} to ${id}.")
        masterId = id
      } else {
        debug(s"Master ${masterId} is still active")
      }
      Thread.sleep(1000)
      findMaster()
    }
  }

  /**
    * Return the list of id with lower priority
    */
  def getNodesIdWithHigherPriority(): List[Int] = ???
}
