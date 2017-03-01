package storm.dataclean.component.bolt.repair;


import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.dataclean.auxiliary.base.Violation;
import storm.dataclean.auxiliary.base.ViolationGroup;
import storm.dataclean.auxiliary.repair.*;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposal;
import storm.dataclean.auxiliary.repair.coordinator.MergeEQClassProposalGroup;
import storm.dataclean.auxiliary.repair.subgraph.AbstractSubGraph;
import storm.dataclean.exceptions.*;
import storm.dataclean.util.BleachConfig;
import java.util.Collection;

/**
 * Created by tian on 01/12/2015.
 * All passing by Repair Coordinator, no matter violations have intersection or not.
 */
public class BasicRepairWorkerBolt extends RepairWorkerBolt {


    public BasicRepairWorkerBolt(BleachConfig bconfig) {
        super(bconfig);
    }

    @Override
    public void process_data(Tuple tuple) throws BleachException {
        ViolationGroup vg = (ViolationGroup) tuple.getValueByField("vg");
        int tid = vg.getTid();
        int kid = vg.getKid();
        addCellVcFromViolations(vg);
        buffered_tid_vlist_map.put(tid, vg);
        MergeEQClassProposalGroup mp = getMergeEQClassProposal(tid, kid, vg);
        _collector.emit(REPAIR_TO_COORDINATE_STREAM_ID, tuple, new Values(mp));
    }

    @Override
    public void process_coordinate(Tuple tuple) {
        RepairProposal rp = new RepairProposal();
        MergeEQClassProposalGroup mp = (MergeEQClassProposalGroup) tuple.getValueByField("merged_propose");
        int tid = mp.getTid();
        int kid = mp.getKid();
        ViolationGroup vg = buffered_tid_vlist_map.get(tid);   // if we first decide merge, what about the new violation?
        try {
            if (mp.isEmpty()) {
                for (int attr : vg.getAttrs()) {
                    Collection<Violation> vios = vg.getViolations(attr);
                    rp.addProposal(getEQclassWithUpdate(tid, vios));
                }
            } else {
                for (int attr : vg.getAttrs()) {
                        Collection<Violation> vios = vg.getViolations(attr);
                        AbstractSubGraph merged_peqclass = getEQclassWithUpdate(tid, vios);
                        if (mp.hasAttr(attr)) {
                            MergeEQClassProposal smp = mp.getSingleMergeEQClassProposal(attr);
                            merged_peqclass = fdrg.getMergedSubGraph(merged_peqclass, smp);
                        }
                        rp.addProposal(merged_peqclass);
                }
            }
        } catch (BleachException e) {
            e.printStackTrace();
        }
        buffered_tid_vlist_map.remove(tid);


        _collector.emit(REPAIR_DATA_STREAM_ID, tuple, new Values(kid, rp));
    }
}