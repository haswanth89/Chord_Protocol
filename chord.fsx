module Chord 

#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
open System
open Akka.FSharp
open Akka.Actor


let mutable flag = true

let r = System.Random()
let system = System.create "system" (Configuration.load())
// Define a message type for queries
type QueryMessage =
    {
        Source: IActorRef
        k: int
        Hop: int
    }

// Define a message type for finger information
type FingerInfo =
    {
        TargetIndex: int
        TargetNode: IActorRef
        FingerFrom: IActorRef
        FingerKey: int
        FingerIndex: int
        IsReturn: bool
    }
// Define a message type for node information
type Nodeinfo = 
    {
        Ndkey: int
        NdRef: IActorRef
    }

// Define a message type for joining information
type JoiningInfo =
    {
        Before: Nodeinfo
        Next: Nodeinfo
    }

// Define a message type for updating predecessor information
type UpdatePredecessor =
    {
        UBefore: Nodeinfo
    }
// Define a message type for setting up node information
type SetupInfo =
    {
        SelfKey: int
        BNodeinfo: Nodeinfo
        NNodeinfo: Nodeinfo
        TableNodeinfo: List<Nodeinfo>
    }
let mutable numlst = []
let mutable chrdactrlist= []
let mutable inpStrList = [[]]
let mutable numNodes = 0
let mutable numRqsts = 0
let mutable m = 20
let mutable maxN = 1 <<< m
 // Helper function to convert a string to a key
let Actorpeer = 
    spawn system "Actorpeer" 
        (fun mailbox ->
            let mutable Totalhops = 0
            let mutable Counter = 0
             // Helper function to convert a string to a key
            let converter (s:string) =
                System.Text.Encoding.ASCII.GetBytes(s)
                |> Array.fold (fun acc byte -> acc + int byte) 0
                |> fun res -> if res < 0 then -res else res    
            // Main loop for the Actorpeer actor
            let rec loop() = actor {
                let! message = mailbox.Receive()
                match box message with
                | :? string as s when s = "begin" ->
                    for i in 0 .. (numNodes - 1) do
                        for j in 0 .. (numRqsts - 1) do
                            chrdactrlist.[i] <! {k = (converter inpStrList.[i].[j]) % maxN; Hop = 0; Source = mailbox.Self}
                    return! loop()

                | :? QueryMessage as q ->
                    Totalhops <- Totalhops + q.Hop
                    Counter <- Counter + 1
                    if Counter = numNodes * numRqsts then
                        printfn "Average Hops: %f" ((float Totalhops) / (float Counter))
                        Environment.Exit(0)
                        return! loop() // This line is technically unreachable due to the exit call, but it's required for the actor computation expression.
                    else
                        return! loop()

                | _ -> return! loop()
            }
            loop()
        )



let chrngActor (mailbox: Actor<_>) =
    let mutable fingerTable = []
    let mutable fingerArray = [||]
    let mutable k = 0
    let mutable next = {Ndkey = 0; NdRef = mailbox.Self}
    let mutable before = {Ndkey = 0; NdRef = mailbox.Self}
     // Helper function to find the closest predecessor node
    let closestbeforeNd id =

        let mutable temp = -1
        for i = m-1 downto 0 do
            let a = if fingerTable.[i].Ndkey < k then fingerTable.[i].Ndkey + maxN else fingerTable.[i].Ndkey
            let id1 = if id < k then id + maxN else id
            if temp = -1 && a > k && a < id1 then
                temp <- i
        if temp = -1 then
            mailbox.Self
        else 
            fingerTable.[temp].NdRef
       
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match box message with
        | :? int as id -> 
            let target = closestbeforeNd id
            if (target) = mailbox.Self then
                  printfn ""
            else
                target <! id

        | :? QueryMessage as q ->
            let target = closestbeforeNd q.k
            if (target) = mailbox.Self then

                  q.Source <! q
            else
                target <! {k = q.k; Hop = q.Hop + 1; Source = q.Source}

        | :? JoiningInfo as itj ->
            before <- itj.Before
            next <- itj.Next
            for i = 0 to m - 1 do
                next.NdRef <! {FingerKey = k + (1 <<< i);FingerIndex = i; FingerFrom = mailbox.Self; IsReturn = false;TargetIndex = -1; TargetNode = mailbox.Self}
        
        | :? SetupInfo as info ->
            k <- info.SelfKey
            next <- info.BNodeinfo
            before <- info.NNodeinfo
            fingerTable <- info.TableNodeinfo

        | :? Nodeinfo as info ->
            let target = closestbeforeNd info.Ndkey
            if target = mailbox.Self then
                let itj = {Before = {Ndkey = k; NdRef = mailbox.Self}; Next = {Ndkey = next.Ndkey; NdRef = next.NdRef}}
                info.NdRef <! itj
                next.NdRef <! {UBefore = info}
                next <- info
                for i = 0 to m - 1 do
                    next.NdRef <! {FingerKey = k + (1 <<< i);FingerIndex = i; FingerFrom = mailbox.Self; IsReturn = false;TargetIndex = -1; TargetNode = mailbox.Self}
            else
                target <! info
        
        | :? FingerInfo as fi ->
            if fi.IsReturn = false then
                let target = closestbeforeNd fi.FingerKey
                if target = mailbox.Self then
                    fi.FingerFrom <! {FingerFrom = fi.FingerFrom; FingerKey = fi.FingerKey; FingerIndex = fi.FingerIndex; IsReturn = true; TargetIndex = next.Ndkey; TargetNode = next.NdRef}
                else
                    target <! fi
            else
                Array.set fingerArray fi.FingerIndex {Ndkey = fi.TargetIndex; NdRef = fi.TargetNode}
        
        | :? string as x -> 
            printfn ""

        | :? UpdatePredecessor as u ->
            before <- u.UBefore
       
        | _ -> ()
            
        return! loop()
    }
    loop()




let connect  numofPeers numofrequests  =
    numNodes <- int numofPeers
    numRqsts <- int numofrequests

    m <- 0
    let mutable n = numNodes
    while n > 0 do
        m <- m + 1
        n <- (n >>> 1)
    maxN <- 1 <<< m
    let rndnumlst = [
       for i in 1 .. numNodes do
            yield (r.Next() % maxN)
    ]
 
    numlst <- List.sort rndnumlst

    chrdactrlist <- [
        for i in 0 .. (numNodes - 1) do
            let name = "Actor" + i.ToString()
            let temp = spawn system name chrngActor
            yield temp
    ]

    for i in 0 .. (numNodes - 1) do
        let FList = [
            for j in 0 .. (m-1) do
                let mutable mrk = -1;
                let temp = 1 <<< j
                for k in 1 .. numNodes do
                    let a = if i + k >= numNodes then numlst.[(i+k) % numNodes] + maxN else numlst.[(i+k) % numNodes]
                    if a >= temp + numlst.[i] && mrk = -1 then
                        mrk <- (i+k) % numNodes
                let temp = {Ndkey = numlst.[mrk]; NdRef = chrdactrlist.[mrk]}
                yield temp
        ]
     
        let myInfo = {SelfKey = numlst.[i]; BNodeinfo = {Ndkey = numlst.[(i+1) % numNodes]; NdRef = chrdactrlist.[(i+1) % numNodes]}; NNodeinfo = {Ndkey = numlst.[(i+numNodes-1) % numNodes]; NdRef = chrdactrlist.[(i+numNodes-1) % numNodes]}; TableNodeinfo = FList}
        chrdactrlist.[i] <! myInfo

    inpStrList <- [
        for i in 1 .. numNodes do
            let rqstList = [
                for j in 1 .. numRqsts do
                    let mutable str = ""
                    let l = r.Next() % 100 + 1
                    for k in 0 .. l do
                        str <- str + char(r.Next() % 95 + 32).ToString()
                    yield str
            ]
            yield rqstList
    ]
    Actorpeer <! "begin"
    let mutable x =1 
    while true do
        x <- 1
