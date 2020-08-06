/**
 * Sales Order Allocation
 * Responsible for allocating shipping costs and discounts to a Campaign.
 *
 * Version      Date                Author                      Remarks
 * 1.0          04 Mar 2020         Chris Abbott                N/A
 * 1.1          06 Aug 2020         Chris Abbott                Allocation of discounts.
 */

function customizeGlImpact(transactionRecord, standardLines, customLines, book) {
    // The Campaign from the first line will be used to allocate the target lines.
    var campaign = transactionRecord.getLineItemValue('item', 'class', 1);

    // We'll find a list of accounts that will be subject to allocation.
    var accounts = [];

    // Get the shipping item account.
    var shipping_item_id = transactionRecord.getFieldValue('shipmethod');
    if (shipping_item_id) {
        var shipping_item = nlapiLoadRecord('shipitem', shipping_item_id);
        accounts.push(Number(shipping_item.getFieldValue('account')));
    }

    // Get the accounts for all discount items.
    var item_list = [];

    // First, use the header discount item.
    var discount_item = transactionRecord.getFieldValue('discountitem');
    if (discount_item) {
        item_list.push(discount_item);
    }

    // Second, look for any line level discounts.
    for (var i=1; i<=transactionRecord.getLineItemCount('item'); i++) {
        item_list.push(transactionRecord.getLineItemValue('item', 'item', i));
    }

    var item_search_results = nlapiSearchRecord(
        'item',
        null,
        [
            ['type', 'anyof', 'Discount'],
            'and',
            ['internalid', 'anyof', item_list],
        ],
        [
            new nlobjSearchColumn('internalid'),
            new nlobjSearchColumn('incomeaccount')
        ]
    );

    if (item_search_results) {
        for (var i = 0; i < item_search_results.length; i++) {
            var account = Number(item_search_results[i].getValue('incomeaccount'));
            if (accounts.indexOf(account) < 0) {
                accounts.push(account);
            }
        }
    }

    // Check whether there's anything to do.
    if (!campaign || accounts.length == 0) {
        return;
    }

    // Loop through the lines and allocate, as necessary.
    for (var i = 0; i < standardLines.getCount(); i++) {
        var currLine = standardLines.getLine(i);
        var currLineAccountId = Number(currLine.getAccountId());
        var currLineDepartment = currLine.getDepartmentId()

        // Check whether this line needs allocating.
        if (accounts.indexOf(currLineAccountId) < 0) {
            continue;
        }

        // Reverse the line and allocate the replacement line.
        var amount;
        var new_credit_line = customLines.addNewLine();
        new_credit_line.setAccountId(currLineAccountId);
        new_credit_line.setDepartmentId(currLineDepartment);
        var new_debit_line = customLines.addNewLine();
        new_debit_line.setAccountId(currLineAccountId);
        new_debit_line.setDepartmentId(currLineDepartment);

        if (currLine.getCreditAmount() > 0) {
            amount = currLine.getCreditAmount();
            new_credit_line.setClassId(Number(campaign));
        } else {
            amount = currLine.getDebitAmount();
            new_debit_line.setClassId(Number(campaign));
        }

        new_debit_line.setDebitAmount(amount);
        new_credit_line.setCreditAmount(amount);
    }
}