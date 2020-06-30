/**
 * Responsible for allocating shipping costs to a Campaign.
 *
 * Version      Date                Author                      Remarks
 * 1.0          04 Mar 2020         Chris Abbott                N/A
 */

function customizeGlImpact(transactionRecord, standardLines, customLines, book) {
    var shipping_item_id = transactionRecord.getFieldValue('shipmethod');
    var shipping_cost = parseFloat(transactionRecord.getFieldValue('shippingcost'));
    var campaign = transactionRecord.getLineItemValue('item', 'class', 1);

    if (!shipping_item_id || shipping_cost == 0 || !campaign) {
        return;
    }

    var shipping_item = nlapiLoadRecord('shipitem', shipping_item_id);
    var shipping_item_ac = shipping_item.getFieldValue('account');

    for (var i = 0; i < standardLines.getCount(); i++) {
        var currLine = standardLines.getLine(i);
        if (currLine.getAccountId() == shipping_item_ac && parseFloat(currLine.getCreditAmount()) == shipping_cost) {
            var new_debit_line = customLines.addNewLine();
            new_debit_line.setAccountId(Number(shipping_item_ac));
            new_debit_line.setDebitAmount(shipping_cost);

            var new_credit_line = customLines.addNewLine();
            new_credit_line.setAccountId(Number(shipping_item_ac));
            new_credit_line.setCreditAmount(shipping_cost);
            new_credit_line.setClassId(Number(campaign));
        }
    }
}