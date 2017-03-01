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
import java.util.Map;

/**
 * Created by tian on 01/12/2015.
 * <p/>
 * Try only delay merge of EQClass, repair decision is made immediate.
 */
public class BleachIRRepairWorkerBolt extends RepairWorkerBolt {

    public BleachIRRepairWorkerBolt(BleachConfig bconfig) {
        super(bconfig);
    }

    public void process_data(Tuple tuple) throws BleachException {
        ViolationGroup vg = (ViolationGroup) tuple.getValueByField("vg");
        addCellVcFromViolations(vg);
        int tid = vg.getTid();
        int kid = vg.getKid();

        RepairProposal rp = new RepairProposal();
        for (int attr : vg.getAttrs()) {
            Collection<Violation> vios = vg.getViolations(attr);
            rp.addProposal(getEQclassWithUpdate(tid, vios));
        }

        if (vg.isRequire_coordinate()) {
            MergeEQClassProposalGroup mp = getMergeEQClassProposal(tid, kid, vg);
            _collector.emit(REPAIR_TO_COORDINATE_STREAM_ID, tuple, new Values(mp));
        }


        _collector.emit(REPAIR_DATA_STREAM_ID, tuple, new Values(kid, rp));
    }

    public void process_coordinate(Tuple tuple) throws ImpossibleException {
        MergeEQClassProposalGroup mp = (MergeEQClassProposalGroup) tuple.getValueByField("merged_propose");
        for(Map.Entry<Integer, MergeEQClassProposal> entry : mp.getAttr_map().entrySet()){
            MergeEQClassProposal smp = entry.getValue();
            AbstractSubGraph merged_peqclass = fdrg.getMergedSubGraph(null, smp);
            if (merged_peqclass == null) {
                throw new ImpossibleException("Merged_peqclass should not be null");
            }
        }
    }


}