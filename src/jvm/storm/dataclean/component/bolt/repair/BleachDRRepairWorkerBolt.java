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
 * Try delay merge of EQClass and repair decision for new violation.
 * But new EQClass is created immediate when receiving new violation.
 */
public class BleachDRRepairWorkerBolt extends RepairWorkerBolt {

    public BleachDRRepairWorkerBolt(BleachConfig bconfig) {
        super(bconfig);
    }

    public void process_data(Tuple tuple) throws BleachException {
        ViolationGroup vg = (ViolationGroup) tuple.getValueByField("vg");
        addCellVcFromViolations(vg);
        int tid = vg.getTid();
        int kid = vg.getKid();

        if (vg.isRequire_coordinate()) {
            MergeEQClassProposalGroup mp = process_data_with_coordinate(tid, kid, vg);
            _collector.emit(REPAIR_TO_COORDINATE_STREAM_ID, tuple, new Values(mp));
        } else {
            RepairProposal rp = process_data_without_coordinate(vg);
            _collector.emit(REPAIR_DATA_STREAM_ID, tuple, new Values(kid, rp));
        }
    }

    public void process_coordinate(Tuple tuple) throws ImpossibleException {
        RepairProposal rp = new RepairProposal();
        MergeEQClassProposalGroup mp = (MergeEQClassProposalGroup) tuple.getValueByField("merged_propose");
        int tid = mp.getTid();
        int kid = mp.getKid();
        ViolationGroup vg = buffered_tid_vlist_map.get(tid);   // if we first decide merge, what about the new violation?
        for (int attr : vg.getAttrs()) {

            if(mp.containAttr(attr)) {
                MergeEQClassProposal smp = mp.getSingleMergeEQClassProposal(attr);
                AbstractSubGraph merged_peqclass = fdrg.getMergedSubGraph(null, smp);
                if (merged_peqclass == null) {
                    throw new ImpossibleException("Merged_peqclass should not be null, as violation should already be added");
                }
                rp.addProposal(merged_peqclass);
            } else {
                rp.addProposal( fdrg.getSubgraph(vg.getViolations(attr).iterator().next().getVioCause()));
            }

        }
        buffered_tid_vlist_map.remove(tid);
        _collector.emit(REPAIR_DATA_STREAM_ID, tuple, new Values(kid, rp));
    }

    public MergeEQClassProposalGroup process_data_with_coordinate(int tid, int kid, ViolationGroup vg) throws BleachException {
        buffered_tid_vlist_map.put(tid, vg);
        for (int attr : vg.getAttrs()) {
            getEQclassWithUpdate(tid, vg.getViolations(attr));
        }
        MergeEQClassProposalGroup mp = getMergeEQClassProposal(tid, kid, vg);
        return mp;
    }

    public RepairProposal process_data_without_coordinate(ViolationGroup vg) throws BleachException {
        RepairProposal rp = new RepairProposal();
        for (int attr : vg.getAttrs()) {
            Collection<Violation> vios = vg.getViolations(attr);
            AbstractSubGraph sg = getEQclassWithUpdate(vg.getTid(), vios);
            rp.addProposal(sg);
        }
        return rp;
    }

}