/*
 * Copyright by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package queryGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import config.Constants;
import datatype.*;

public class RandomQueryGenerator {

    DateTimeArgument START_DATE;
    DateTimeArgument END_DATE;
    long MAX_ID;
    long MIN_ID;

    Random rand;
    long seed;
    ArrayList<IArgument> args;

    public static QueryParamSetting qps;

    public RandomQueryGenerator(long seed, long maxUsrId) {
        this.rand = new Random(seed);
        setStartDate(ArgumentParser.parseDateTime(Constants.DEFAULT_START_DATE));
        setEndDate(ArgumentParser.parseDateTime(Constants.DEFAULT_END_DATE));
        setMaxFbuId(maxUsrId);
        this.args = new ArrayList<IArgument>();
    }
    public RandomQueryGenerator(long seed, long minUserId,long maxUsrId) {
        this(seed,maxUsrId);
        this.MIN_ID=minUserId;
        this.MAX_ID = maxUsrId;
    }
    public ArrayList<IArgument> nextQuery(int qIx, int vIx) {
        args.clear();
        switch (qIx) {
            case 11:
            case 12:
                nextQ11(qIx, vIx);
                break;
            case 10:
                nextQ10(qIx,vIx);
                break;
            case 100000:
            case 100001:
            case 100002:
            case 100003:
            case 100004:
            case 100005:
            case 100006:
            case 200001:
            case 200002:
            case 200003:
            case 200004:
            case 200005:
            case 200006:
            case 200007:
            case 300000:
            case 300001:
            case 300002:
            case 300003:
            case 300004:
            case 300005:
            case 300006:
            case 300007:
            case 400001:
            case 400002:
            case 400003:
            case 400004:
            case 400005:
            case 400006:
            case 500007:
            case 500008:
            case 500009:
            case 500010:
            case 500011:
            case 500012:
            case 500013:
            case 600007:
            case 600008:
            case 600009:
            case 600010:
            case 600011:
            case 600012:
            case 600013:
            case 99999:
                nextQRandMulti(qIx,vIx);
                break;
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 8:
        case 940000:
            //Queries with no params
            nextQ1();
            break;
        case 2000:
        case 2001:
        case 2002:
        case 2003:
            nextQ2K(qIx, vIx);
            break;
        case 7:
            nextQ7(qIx, vIx);
            break;
        case 1000:
        case 1001:
        case 1003:
        case 1004:
        case 1005:
        case 1006:
            case 1007:
            nextThousand(qIx,vIx);
            break;
        case 8000:
            case 8001:
            case 8002:
            case 8003:
            case 8004:
            case 8005:
            case 9000:
            case 9001:
            case 9002:
            case 9003:
            case 9004:
            case 9005:
            case 11000:
            case 11001:
            case 11002:
            case 11003:
            case 11004:
            case 11005:
            case 11006:
            case 11007:
            case 11008:
            case 11009:
            case 11010:
            case 12000:
            case 12001:
            case 12002:
            case 12003:
            case 12004:
            case 12005:
            case 12006:
            case 12007:
            case 12008:
            case 12009:
            case 12010:
                nextQ8kAnd9k(qIx,vIx);
                break;
            case 10000:
            case 10001:
            case 10002:
            case 10003:
            case 10004:
            case 10005:
            case 13000:
            case 13001:
            case 13002:
            case 13003:
            case 13004:
            case 13005:
            case 13006:
            case 13007:
            case 13008:
            case 13009:
            case 13010:
                nextQ10K(qIx,vIx);
                break;
            case 50000:
                nextQ50k(qIx,vIx);
                break;
            case 50004:
            case 50005:
            case 50007:
            case 50008:
            case 50009:
            case 50010:
            case 50011:
                nextQFF_N_RANDOM_N(qIx,vIx);
                break;
            case 2004:
                nextQAPPENDK(qIx,vIx);
                break;
            case 60000:
            case 60001:
            case 70000:
                nextQ60000(qIx, vIx);
                break;
            case 80000:
            case 80001:
                nextQ80k(qIx,vIx);
                break;
            case 910001:
            case 910003:
            case 910004:
            case 910005:
            case 910006:
            case 910007:
            case 910008:
            case 910011:
            case 910013:
            case 910014:
            case 910015:
            case 910016:
            case 910017:
            case 910018:

                nextQRDTEXPR3(qIx, vIx);
                break;
            case 910002:
            case 910012:
                nextQLDTEXPR3(qIx, vIx);
                break;
            case 400007:
            case 910019:
                nextQBUSHYEXPR3(qIx, vIx);
                break;
            case 920000:
                nextTpch8LDT(qIx, vIx);
                break;
            case 920001:
            case 920002:
            case 920003:
            case 920004:
            case 920005:
            case 920006:
            case 920007:
            case 920008:
            case 920009:
            case 920010:
            case 920011:
            case 920012:
                nextTpch8RDT(qIx, vIx);
            break;
            case 930000:
                nextTpch9LDT(qIx, vIx);
                break;
            case 930001:
            case 930002:
            case 930003:
            case 930004:
            case 930005:
            case 930006:
            case 930007:
                nextTpch9RDT(qIx, vIx);
                break;
            case 950000:
                nextQ950000(qIx,vIx);
                break;
            case 950001:
                nextQ950001(qIx, vIx);
                break;
            case 960000:
                nextQ960000(qIx, vIx);
                break;
            case 960001:
                nextQ960001(qIx, vIx);
                break;
            case 960002:
                nextQ960002(qIx, vIx);
                break;
        default:
            next(qIx,vIx);
            break;
        }
        return (new ArrayList<IArgument>(args));
    }


    private void nextQRandMulti(int qid, int vid) {
        Random rand = new Random();
        //get threadId
        System.out.println(Thread.currentThread().getName());
        int threadId = Integer.parseInt(Thread.currentThread().getName().split("Client-")[1]);
        System.out.println(threadId);
        System.out.println(Thread.currentThread().getName());
        String joinMemory = (String)qps.getParam(qid, vid).get(0);
        int numberOfDS = (Integer)qps.getParam(qid, vid).get(1);
        int totalNumberOfDS = (Integer)qps.getParam(qid, vid).get(2);
        System.out.println(totalNumberOfDS);
        args.add(new StringArgument(joinMemory));
        Set<Integer> dsNumbers = new HashSet<Integer>();
        for (int i = 0; i < numberOfDS; i++) {
            int ds = (rand.nextInt()%totalNumberOfDS)+1 +(threadId*totalNumberOfDS);
            while (!dsNumbers.add(ds) || ds < 1) {
                ds = (rand.nextInt()%totalNumberOfDS)+1 + (threadId*totalNumberOfDS);
            }
            System.out.println("threadID: "+threadId+" ds"+i+": "+ds);
//            int ds = i+1;
            args.add(new StringArgument("wisconsin_fixed_record_size_1GB_1000000_ds"+ds));
        }

    }


    private void nextQ950000(int qid, int vid){
        String joinMem = (String)qps.getParam(qid, vid).get(0);
        String rel1 = (String)qps.getParam(qid, vid).get(1);
        int bSize = (Integer)qps.getParam(qid, vid).get(2);
        String rel2 = (String)qps.getParam(qid, vid).get(3);
        args.add(new StringArgument(joinMem));
        args.add(new StringArgument(rel1));
        args.add(new StringArgument(rel2));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));
        args.add(new IntArgument(bSize));

    }

    private void nextQ950001(int qid, int vid){
        String joinMem = (String)qps.getParam(qid, vid).get(0);
        String rel1 = (String)qps.getParam(qid, vid).get(1);
        String rel2 = (String)qps.getParam(qid, vid).get(2);
        args.add(new StringArgument(joinMem));
        args.add(new StringArgument(rel1));
        args.add(new StringArgument(rel2));

    }
    private void nextTpch8RDT(int qid, int vid) {
        String seqbuild = (String)qps.getParam(qid, vid).get(0);
        int j1BuildSize = (Integer)qps.getParam(qid, vid).get(1);
        int j1JoinMemory=(Integer)qps.getParam(qid, vid).get(2);
        int j2BuildSize = (Integer)qps.getParam(qid, vid).get(3);
        int j2JoinMemory=(Integer)qps.getParam(qid, vid).get(4);
        int j3BuildSize = (Integer)qps.getParam(qid, vid).get(5);
        int j3JoinMemory=(Integer)qps.getParam(qid, vid).get(6);
        int j4BuildSize = (Integer)qps.getParam(qid, vid).get(7);
        int j4JoinMemory=(Integer)qps.getParam(qid, vid).get(8);
        int j5BuildSize = (Integer)qps.getParam(qid, vid).get(9);
        int j5JoinMemory=(Integer)qps.getParam(qid, vid).get(10);
        int j6BuildSize = (Integer)qps.getParam(qid, vid).get(11);
        int j6JoinMemory=(Integer)qps.getParam(qid, vid).get(12);
        int j7BuildSize = (Integer)qps.getParam(qid, vid).get(13);
        int j7JoinMemory=(Integer)qps.getParam(qid, vid).get(14);

        args.add(new StringArgument(seqbuild));
        args.add(new IntArgument(j1BuildSize));
        args.add(new IntArgument(j1JoinMemory));
        args.add(new IntArgument(j2BuildSize));
        args.add(new IntArgument(j2JoinMemory));
        args.add(new IntArgument(j3BuildSize));
        args.add(new IntArgument(j3JoinMemory));
        args.add(new IntArgument(j4BuildSize));
        args.add(new IntArgument(j4JoinMemory));
        args.add(new IntArgument(j5BuildSize));
        args.add(new IntArgument(j5JoinMemory));
        args.add(new IntArgument(j6BuildSize));
        args.add(new IntArgument(j6JoinMemory));
        args.add(new IntArgument(j7BuildSize));
        args.add(new IntArgument(j7JoinMemory));
    }
    private void nextTpch8LDT(int qid, int vid) {

        int j1BuildSize = (Integer)qps.getParam(qid, vid).get(0);
        int j1JoinMemory=(Integer)qps.getParam(qid, vid).get(1);
        int j2BuildSize = (Integer)qps.getParam(qid, vid).get(2);
        int j2JoinMemory=(Integer)qps.getParam(qid, vid).get(3);
        int j3BuildSize = (Integer)qps.getParam(qid, vid).get(4);
        int j3JoinMemory=(Integer)qps.getParam(qid, vid).get(5);
        int j4BuildSize = (Integer)qps.getParam(qid, vid).get(6);
        int j4JoinMemory=(Integer)qps.getParam(qid, vid).get(7);
        int j5BuildSize = (Integer)qps.getParam(qid, vid).get(8);
        int j5JoinMemory=(Integer)qps.getParam(qid, vid).get(9);
        int j6BuildSize = (Integer)qps.getParam(qid, vid).get(10);
        int j6JoinMemory=(Integer)qps.getParam(qid, vid).get(11);
        int j7BuildSize = (Integer)qps.getParam(qid, vid).get(12);
        int j7JoinMemory=(Integer)qps.getParam(qid, vid).get(13);

        args.add(new IntArgument(j1BuildSize));
        args.add(new IntArgument(j1JoinMemory));
        args.add(new IntArgument(j2BuildSize));
        args.add(new IntArgument(j2JoinMemory));
        args.add(new IntArgument(j3BuildSize));
        args.add(new IntArgument(j3JoinMemory));
        args.add(new IntArgument(j4BuildSize));
        args.add(new IntArgument(j4JoinMemory));
        args.add(new IntArgument(j5BuildSize));
        args.add(new IntArgument(j5JoinMemory));
        args.add(new IntArgument(j6BuildSize));
        args.add(new IntArgument(j6JoinMemory));
        args.add(new IntArgument(j7BuildSize));
        args.add(new IntArgument(j7JoinMemory));
    }

    private void nextTpch9RDT(int qid, int vid) {
        String seqbuild = (String)qps.getParam(qid, vid).get(0);
        int j1BuildSize = (Integer)qps.getParam(qid, vid).get(1);
        int j1JoinMemory=(Integer)qps.getParam(qid, vid).get(2);
        int j2BuildSize = (Integer)qps.getParam(qid, vid).get(3);
        int j2JoinMemory=(Integer)qps.getParam(qid, vid).get(4);
        int j3BuildSize = (Integer)qps.getParam(qid, vid).get(5);
        int j3JoinMemory=(Integer)qps.getParam(qid, vid).get(6);
        int j4BuildSize = (Integer)qps.getParam(qid, vid).get(7);
        int j4JoinMemory=(Integer)qps.getParam(qid, vid).get(8);
        int j5BuildSize = (Integer)qps.getParam(qid, vid).get(9);
        int j5JoinMemory=(Integer)qps.getParam(qid, vid).get(10);
        int j6BuildSize = (Integer)qps.getParam(qid, vid).get(11);
        int j6JoinMemory=(Integer)qps.getParam(qid, vid).get(12);


        args.add(new StringArgument(seqbuild));
        args.add(new IntArgument(j1BuildSize));
        args.add(new IntArgument(j1JoinMemory));
        args.add(new IntArgument(j2BuildSize));
        args.add(new IntArgument(j2JoinMemory));
        args.add(new IntArgument(j3BuildSize));
        args.add(new IntArgument(j3JoinMemory));
        args.add(new IntArgument(j4BuildSize));
        args.add(new IntArgument(j4JoinMemory));
        args.add(new IntArgument(j5BuildSize));
        args.add(new IntArgument(j5JoinMemory));
        args.add(new IntArgument(j6BuildSize));
        args.add(new IntArgument(j6JoinMemory));

    }
    private void nextTpch9LDT(int qid, int vid) {

        int j1BuildSize = (Integer)qps.getParam(qid, vid).get(0);
        int j1JoinMemory=(Integer)qps.getParam(qid, vid).get(1);
        int j2BuildSize = (Integer)qps.getParam(qid, vid).get(2);
        int j2JoinMemory=(Integer)qps.getParam(qid, vid).get(3);
        int j3BuildSize = (Integer)qps.getParam(qid, vid).get(4);
        int j3JoinMemory=(Integer)qps.getParam(qid, vid).get(5);
        int j4BuildSize = (Integer)qps.getParam(qid, vid).get(6);
        int j4JoinMemory=(Integer)qps.getParam(qid, vid).get(7);
        int j5BuildSize = (Integer)qps.getParam(qid, vid).get(8);
        int j5JoinMemory=(Integer)qps.getParam(qid, vid).get(9);
        int j6BuildSize = (Integer)qps.getParam(qid, vid).get(10);
        int j6JoinMemory=(Integer)qps.getParam(qid, vid).get(11);

        args.add(new IntArgument(j1BuildSize));
        args.add(new IntArgument(j1JoinMemory));
        args.add(new IntArgument(j2BuildSize));
        args.add(new IntArgument(j2JoinMemory));
        args.add(new IntArgument(j3BuildSize));
        args.add(new IntArgument(j3JoinMemory));
        args.add(new IntArgument(j4BuildSize));
        args.add(new IntArgument(j4JoinMemory));
        args.add(new IntArgument(j5BuildSize));
        args.add(new IntArgument(j5JoinMemory));
        args.add(new IntArgument(j6BuildSize));
        args.add(new IntArgument(j6JoinMemory));

    }

    private void nextQRDTEXPR3(int qid, int vid) {
        int recordSize = (Integer)qps.getParam(qid, vid).get(0);
        String seqbuild = (String)qps.getParam(qid, vid).get(1);
        int j1BuildSize1 = (Integer)qps.getParam(qid, vid).get(2)* recordSize/1024/1024;
        int j1JoinMemory1=(Integer)qps.getParam(qid, vid).get(3)* recordSize/1024/1024;
        int j1BuildSize2 = (Integer)qps.getParam(qid, vid).get(4)* recordSize/1024/1024;
        int j1JoinMemory2=(Integer)qps.getParam(qid, vid).get(5)* recordSize/1024/1024;
        int j2BuildSize1 = (Integer)qps.getParam(qid, vid).get(6)* recordSize/1024/1024;
        int j2JoinMemory1=(Integer)qps.getParam(qid, vid).get(7)* recordSize/1024/1024;
        int j2BuildSize2 = (Integer)qps.getParam(qid, vid).get(8)* recordSize/1024/1024;
        int j2JoinMemory2=(Integer)qps.getParam(qid, vid).get(9)* recordSize/1024/1024;
        int j3BuildSize1 = (Integer)qps.getParam(qid, vid).get(10)* recordSize/1024/1024;
        int j3JoinMemory1=(Integer)qps.getParam(qid, vid).get(11)* recordSize/1024/1024;
        int j3BuildSize2 = (Integer)qps.getParam(qid, vid).get(12)* recordSize/1024/1024;
        int j3JoinMemory2=(Integer)qps.getParam(qid, vid).get(13)* recordSize/1024/1024;
        int j4BuildSize1 = (Integer)qps.getParam(qid, vid).get(14)* recordSize/1024/1024;
        int j4JoinMemory1=(Integer)qps.getParam(qid, vid).get(15)* recordSize/1024/1024;
        int j4BuildSize2 = (Integer)qps.getParam(qid, vid).get(16)* recordSize/1024/1024;
        int j4JoinMemory2=(Integer)qps.getParam(qid, vid).get(17)* recordSize/1024/1024;
        args.add(new StringArgument(seqbuild));
        args.add(new IntArgument(j1BuildSize1));
        args.add(new IntArgument(j1JoinMemory1));
        args.add(new IntArgument(j1BuildSize2));
        args.add(new IntArgument(j1JoinMemory2));
        args.add(new IntArgument(j2BuildSize1));
        args.add(new IntArgument(j2JoinMemory1));
        args.add(new IntArgument(j2BuildSize2));
        args.add(new IntArgument(j2JoinMemory2));
        args.add(new IntArgument(j3BuildSize1));
        args.add(new IntArgument(j3JoinMemory1));
        args.add(new IntArgument(j3BuildSize2));
        args.add(new IntArgument(j3JoinMemory2));
        args.add(new IntArgument(j4BuildSize1));
        args.add(new IntArgument(j4JoinMemory1));
        args.add(new IntArgument(j4BuildSize2));
        args.add(new IntArgument(j4JoinMemory2));
    }

    private void nextQLDTEXPR3(int qid, int vid) {
        int recordSize = (Integer)qps.getParam(qid, vid).get(0);
        int j1BuildSize1 = (Integer)qps.getParam(qid, vid).get(1)* recordSize/1024/1024;
        int j1JoinMemory1=(Integer)qps.getParam(qid, vid).get(2)* recordSize/1024/1024;
        int j1BuildSize2 = (Integer)qps.getParam(qid, vid).get(3)* recordSize/1024/1024;
        int j1JoinMemory2=(Integer)qps.getParam(qid, vid).get(4)* recordSize/1024/1024;
        int j2BuildSize1 = (Integer)qps.getParam(qid, vid).get(5)* recordSize/1024/1024;
        int j2JoinMemory1=(Integer)qps.getParam(qid, vid).get(6)* recordSize/1024/1024;
        int j2BuildSize2 = (Integer)qps.getParam(qid, vid).get(7)* recordSize/1024/1024;
        int j2JoinMemory2=(Integer)qps.getParam(qid, vid).get(8)* recordSize/1024/1024;
        int j3BuildSize1 = (Integer)qps.getParam(qid, vid).get(9)* recordSize/1024/1024;
        int j3JoinMemory1=(Integer)qps.getParam(qid, vid).get(10)* recordSize/1024/1024;
        int j3BuildSize2 = (Integer)qps.getParam(qid, vid).get(11)* recordSize/1024/1024;
        int j3JoinMemory2=(Integer)qps.getParam(qid, vid).get(12)* recordSize/1024/1024;
        int j4BuildSize1 = (Integer)qps.getParam(qid, vid).get(13)* recordSize/1024/1024;
        int j4JoinMemory1=(Integer)qps.getParam(qid, vid).get(14)* recordSize/1024/1024;
        int j4BuildSize2 = (Integer)qps.getParam(qid, vid).get(15)* recordSize/1024/1024;
        int j4JoinMemory2=(Integer)qps.getParam(qid, vid).get(16)* recordSize/1024/1024;
        args.add(new IntArgument(j1BuildSize1));
        args.add(new IntArgument(j1JoinMemory1));
        args.add(new IntArgument(j1BuildSize2));
        args.add(new IntArgument(j1JoinMemory2));
        args.add(new IntArgument(j2BuildSize1));
        args.add(new IntArgument(j2JoinMemory1));
        args.add(new IntArgument(j2BuildSize2));
        args.add(new IntArgument(j2JoinMemory2));
        args.add(new IntArgument(j3BuildSize1));
        args.add(new IntArgument(j3JoinMemory1));
        args.add(new IntArgument(j3BuildSize2));
        args.add(new IntArgument(j3JoinMemory2));
        args.add(new IntArgument(j4BuildSize1));
        args.add(new IntArgument(j4JoinMemory1));
        args.add(new IntArgument(j4BuildSize2));
        args.add(new IntArgument(j4JoinMemory2));
    }

    private void nextQBUSHYEXPR3(int qid, int vid) {
        int recordSize = (Integer)qps.getParam(qid, vid).get(0);
        int j1JoinMemory1=(Integer)qps.getParam(qid, vid).get(1)* recordSize/1024/1024/4;
        int j1BuildSize1 = (Integer)qps.getParam(qid, vid).get(2)* recordSize/1024/1024;
        int j1BuildSize2 = (Integer)qps.getParam(qid, vid).get(3)* recordSize/1024/1024;
        int j2BuildSize1 = (Integer)qps.getParam(qid, vid).get(4)* recordSize/1024/1024;
        int j2BuildSize2 = (Integer)qps.getParam(qid, vid).get(5)* recordSize/1024/1024;
        int j3BuildSize1 = (Integer)qps.getParam(qid, vid).get(6)* recordSize/1024/1024;
        int j3BuildSize2 = (Integer)qps.getParam(qid, vid).get(7)* recordSize/1024/1024;
        int j4BuildSize1 = (Integer)qps.getParam(qid, vid).get(8)* recordSize/1024/1024;
        int j4BuildSize2 = (Integer)qps.getParam(qid, vid).get(9)* recordSize/1024/1024;
        args.add(new StringArgument(j1JoinMemory1+"MB"));
        args.add(new IntArgument(j1BuildSize1));
        args.add(new IntArgument(j1BuildSize2));
        args.add(new IntArgument(j2BuildSize1));
        args.add(new IntArgument(j2BuildSize2));
        args.add(new IntArgument(j3BuildSize1));
        args.add(new IntArgument(j3BuildSize2));
        args.add(new IntArgument(j4BuildSize1));
        args.add(new IntArgument(j4BuildSize2));
    }

    private void nextQ1() {}
    private void nextQ11(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        args.add(new StringArgument(memory));
    }
    private void nextQ10(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        int numPartitions = (Integer)qps.getParam(qid, vid).get(1);
        args.add(new StringArgument(memory));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
        args.add(new IntArgument(numPartitions));
    }

    private void nextQFF_N_RANDOM_N(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(1);
        String ds2 = (String) qps.getParam(qid,vid).get(2);
        int buildSize = (Integer)qps.getParam(qid, vid).get(3);
        Number percentage = (Number)qps.getParam(qid, vid).get(4);
        args.add(new StringArgument(memory));
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
        args.add(new IntArgument(buildSize));
        args.add(new DoubleArgument((double)percentage));
    }
    private void nextQAPPENDK(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(1);
        String ds2 = (String) qps.getParam(qid,vid).get(2);
        int buildSize = (Integer)qps.getParam(qid, vid).get(3);
        int checkFrames = (Integer)qps.getParam(qid, vid).get(4);
        args.add(new StringArgument(memory));
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
        args.add(new IntArgument(buildSize));
        args.add(new IntArgument(checkFrames));
    };



    private void nextQ2K(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(1);
        String ds2 = (String) qps.getParam(qid,vid).get(2);
        int buildSize = (Integer)qps.getParam(qid, vid).get(3);
        args.add(new StringArgument(memory));
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
        args.add(new IntArgument(buildSize));
    }
    private void nextQ7(int qid, int vid) {
        Number percentage = (Number)qps.getParam(qid, vid).get(0);
        long len = (long)(MAX_ID * (double)percentage);
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);
        args.add(s);
        args.add(e);
    }

    private void nextQ8kAnd9k(int qid, int vid) {
        Number percentage = (Number)qps.getParam(qid, vid).get(0);
        DoubleArgument d = new DoubleArgument((double)percentage);
        args.add(d);
    }

    private void nextQ10K(int qid, int vid) {
        Number percentage = (Number)qps.getParam(qid, vid).get(0);
        Number threshold = (Number)qps.getParam(qid, vid).get(1);
        DoubleArgument d = new DoubleArgument((double)percentage);
        args.add(d);
        DoubleArgument t = new DoubleArgument((double)threshold);
        args.add(t);
    }

    private void next(int qid, int vid){
        ArrayList<Object> p = qps.getParam(qid,vid);
//        args.add(new IntArgument((Integer)p.get(0)));
//        args.add(new StringArgument((String)p.get(1)));
        for(Object element:p){
            args.add(new StringArgument((String)element));
        }
    }

    private void nextQ50k(int qid, int vid) {
        String ds1 = (String)qps.getParam(qid, vid).get(0);
        String ds2 = (String)qps.getParam(qid, vid).get(1);
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
    }

    private void nextThousand(int qid,int vid) {
        ArrayList<Object> p = qps.getParam(qid,vid);
        for(Object element:p){
            args.add(new StringArgument("w"+element.toString()));
        }
    }

    private void nextQ60000(int qid, int vid) {

        String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(1);
        String ds2 = (String) qps.getParam(qid,vid).get(2);
        String jk1 = (String) qps.getParam(qid,vid).get(3);
        int buildSize =  (Integer)qps.getParam(qid, vid).get(4);
        String victim = (String) qps.getParam(qid,vid).get(5);
        String jk2 = (String) qps.getParam(qid,vid).get(6);

        args.add(new StringArgument(memory));
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
        args.add(new StringArgument(jk1));
        args.add(new IntArgument(buildSize));
        args.add(new StringArgument(victim));
        args.add(new StringArgument(jk2));
    }

    private void nextQ80k(int qid, int vid) {
        String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(1);
        String ds2 = (String) qps.getParam(qid,vid).get(2);

        args.add(new StringArgument(memory));
        args.add(new StringArgument(ds1));
        args.add(new StringArgument(ds2));
    }

    private void nextQ960000(int qid,int vid){
        //String memory = (String) qps.getParam(qid,vid).get(0);
        String ds1 = (String) qps.getParam(qid,vid).get(0);
        String ds2 = (String) qps.getParam(qid,vid).get(1);
        Number lowRange = (Number)qps.getParam(qid, vid).get(2);
        Number highRange = (Number)qps.getParam(qid, vid).get(3);
        double percentage = generateRandomDouble((double)lowRange,(double)highRange);
        int memory = (int)Math.ceil(percentage*13*1024)/4; //14GB is size of workspace. 4data partitions
        String memoryArg = "\""+memory+"MB\"";
        double recordPercentage = memory*4.0/(44*1024*1.0); //40  GB is size of the dataset
        long len = (long)(MAX_ID * (double)recordPercentage);
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);


        int ds1_posix = 1+rand.nextInt(5);//between [0,6)
        int ds2_posix = ds1_posix;
        while(ds2_posix==ds1_posix){
            ds2_posix=1+rand.nextInt(5);
        }
        args.add(new StringArgument(memoryArg));
        args.add(new StringArgument(ds1+ds1_posix));
        args.add(new StringArgument(ds2+ds2_posix));
        args.add(s);//ds1
        args.add(e);//ds1
        args.add(s);//ds2
        args.add(e);//ds2
    }

    private void nextQ960001(int qid,int vid){
        String ds1 = (String) qps.getParam(qid,vid).get(0);
        Long len = ((Number)qps.getParam(qid, vid).get(1)).longValue();
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);


        int ds1_posix = 1+rand.nextInt(5);//between [0,6)
        args.add(new StringArgument(ds1+ds1_posix));
        args.add(s);//ds1
        args.add(e);//ds1
    }

    private void nextQ960002(int qid,int vid){
        String ds1 = (String) qps.getParam(qid,vid).get(0);
        Number percentage = (Number)qps.getParam(qid, vid).get(1);
        long len = (long)(MAX_ID * percentage.doubleValue());
        LongArgument s = randomLongArg(1,MAX_ID - len);
        LongArgument e = new LongArgument(s.getValue() + len);


        int ds1_posix = 1+rand.nextInt(5);//between [0,6)
        args.add(new StringArgument(ds1+ds1_posix));
        args.add(s);//ds1
        args.add(e);//ds1
    }
    //Utility Methods
    public void setStartDate(DateTimeArgument sd) {
        this.START_DATE = sd;
    }

    public void setEndDate(DateTimeArgument ed) {
        this.END_DATE = ed;
    }

    public void setMaxFbuId(long max) {
        this.MAX_ID = max;
    }

    public void setQParamSetting(QueryParamSetting qps) {
        this.qps = qps;
    }

    private LongArgument randomLongArg(long min, long max) {
        if(min<1){
            min =1;
        }
        return new LongArgument(generateRandomLong(min, max));
    }


    private long generateRandomLong(long x, long y) {
        return (x + ((long) (rand.nextDouble() * (y - x))));
    }

    private double generateRandomDouble(double x, double y) {
        return (x + (rand.nextDouble() * (y - x)));
    }

}
