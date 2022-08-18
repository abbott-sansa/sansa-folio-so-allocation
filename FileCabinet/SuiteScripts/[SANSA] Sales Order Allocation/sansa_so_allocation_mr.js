/**
 * Sales Order Allocation
 * Automatically codes each Sales Order line to a Marketing Campaign.
 * N.B. Whilst designed with a specific set of scenarios in mind, the intention
 *      is that this customisation may be easily modified.
 *
 * Version      Date                Author                      Remarks
 * 1.0          04 Mar 2020         Chris Abbott                N/A
 * 1.1          01 Jul 2020         Chris Abbott                Moved Lot Number and Analysis Code automation to a separate script.
 * 1.2          05 Oct 2020         Chris Abbott                Filtered out some inactive records in Saved Searches.
 * 1.3          18 Aug 2021         Chris Abbott                Added support for CURRENT allocation rule group.
 * 1.4          14 Mar 2022         Chris Abbott                Fixed some issues and included cache for allocation rules.
 *                                                              Refactored getCampaigns due to an issue with vars.
 * 1.5          11 Aug 2022         Chris Abbott                Updated to reflect the latest allocation requirements.
 *
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define([
    'N/email',
    'N/error',
    'N/file',
    'N/record',
    'N/render',
    'N/runtime',
    'N/search',
    'N/format',
    'N/cache'
], function (email, error, file, record, render, runtime, search, format, cache) {
    // Function used to determine the Campaign for each line in the items sublist.
    function getCampaigns(loaded_record) {
        let campaigns = {};

        let so_allocation_cache = cache.getCache({
            name: 'so_allocation_cache',
            scope: cache.Scope.PUBLIC
        });

        let allocation_rules = JSON.parse(
            so_allocation_cache.get({
                key: 'allocation_rules',
                loader: cacheLoader
            })
        );

        log.debug('allocation_rules', allocation_rules);

        let order_within_twelve_months;
        let live_campaign;
        let previous_order;
        for (let allocation_rule of allocation_rules) {
            log.debug({ title: allocation_rule.rule_group, details: allocation_rule.rules_list });

            // Using try/catch after the decision to continue down the list in the event of any error.
            let allocation_rule_group_name = allocation_rule.rule_group;
            let allocation_rule_group = allocation_rule.rules_list;

            if (allocation_rule_group === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::allocation_rule_group is a required argument.'
                });
            }
            if (allocation_rule_group_name === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::allocation_rule_group_name is a required argument.'
                });
            }
            if (loaded_record === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::loaded_record is a required argument.'
                });
            }

            log.audit('INFO', 'Evaluating Rule Group [' + allocation_rule_group_name + '].');
            switch (allocation_rule_group_name) {
                // Apart from the default case, all others implement custom logic.
                case 'VOUCHER CODES': {
                    // Check whether the Voucher Code is associated with a Campaign.
                    if (allocation_rule_group.length !== 1 || !allocation_rule_group[0].field_id) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message:
                                'Group [VOUCHER CODES] must contain exactly one Allocation Rule and specify a field ID.'
                        });
                    }

                    let voucher_code = loaded_record.getValue({
                        fieldId: allocation_rule_group[0].field_id.toString().toLowerCase()
                    });
                    if (!voucher_code) {
                        break;
                    }

                    let campaign_value;
                    search
                        .create({
                            type: 'customrecordfsl_vouchercode',
                            columns: ['custrecordfsl_campaign'],
                            filters: [
                                ['isinactive', search.Operator.IS, false],
                                'and',
                                ['custrecord118', search.Operator.IS, voucher_code]
                            ]
                        })
                        .run()
                        .each(function (result) {
                            if (campaign_value) {
                                throw error.create({
                                    name: 'VOUCHER_CODE_NOT_UNIQUE',
                                    message: 'Unable to uniquely identify a voucher code as multiple were found.'
                                });
                            }
                            campaign_value = [result.getValue({ name: 'custrecordfsl_campaign' })];

                            return true;
                        });

                    if (campaign_value) {
                        campaigns['*'] = campaign_value;
                    }

                    break;
                }
                case 'NEW CUSTOMER': {
                    if (allocation_rule_group.length !== 1  || !allocation_rule_group[0].campaign) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [NEW CUSTOMER] must contain exactly one Allocation Rule and specify a Campaign.'
                        });
                    }

                    let customer_id = loaded_record.getValue({ fieldId: 'entity' });
                    let transaction_date = loaded_record.getValue({ fieldId: 'trandate' });
                    let transaction_date_string =
                        transaction_date.getFullYear() +
                        '-' +
                        ('0' + (transaction_date.getMonth() + 1)).slice(-2) +
                        '-' +
                        ('0' + transaction_date.getDate()).slice(-2);

                    log.debug({
                        title: 'transaction_date_string',
                        details: transaction_date_string
                    });

                    log.debug({
                        title: 'loaded_record.id',
                        details: loaded_record.id
                    });

                    if (typeof previous_order === 'undefined') {
                        previous_order = false;
                        search
                            .create({
                                type: search.Type.SALES_ORDER,
                                columns: ['internalid'],
                                filters: [
                                    ['internalidnumber', search.Operator.LESSTHAN, loaded_record.id],
                                    'and',
                                    ['customer.internalid', search.Operator.ANYOF, customer_id]
                                ]
                            })
                            .run()
                            .each(function (result) {
                                previous_order = true;

                                return false;
                            });
                    }

                    log.debug({
                        title: 'previous_order',
                        details: previous_order
                    });

                    if (!previous_order) {
                        campaigns['*'] = [allocation_rule_group[0].campaign];
                    }

                    break;
                }
                case 'CURRENT CORE': {
                    if (allocation_rule_group.length !== 1  || !allocation_rule_group[0].campaign) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [CURRENT CORE] must contain exactly one Allocation Rule and specify a Campaign.'
                        });
                    }

                    let customer_id = loaded_record.getValue({ fieldId: 'entity' });
                    let transaction_date = loaded_record.getValue({ fieldId: 'trandate' });
                    let transaction_date_string =
                        transaction_date.getFullYear() +
                        '-' +
                        ('0' + (transaction_date.getMonth() + 1)).slice(-2) +
                        '-' +
                        ('0' + transaction_date.getDate()).slice(-2);

                    log.debug({
                        title: 'transaction_date_string',
                        details: transaction_date_string
                    });

                    log.debug({
                        title: 'loaded_record.id',
                        details: loaded_record.id
                    });

                    if (typeof order_within_twelve_months === 'undefined') {
                        order_within_twelve_months = false;
                        search
                            .create({
                                type: search.Type.SALES_ORDER,
                                columns: ['internalid'],
                                filters: [
                                    ['internalidnumber', search.Operator.LESSTHAN, loaded_record.id],
                                    'and',
                                    ['customer.internalid', search.Operator.ANYOF, customer_id],
                                    'and',
                                    [
                                        `formulanumeric: CASE WHEN MONTHS_BETWEEN(TO_DATE('${transaction_date_string}','YYYY-MM-DD'),{trandate})<=12 THEN 1 ELSE 0 END`,
                                        search.Operator.EQUALTO,
                                        1
                                    ]
                                ]
                            })
                            .run()
                            .each(function (result) {
                                order_within_twelve_months = true;

                                return false;
                            });
                    }

                    if (typeof live_campaign === 'undefined') {
                        live_campaign = false;
                        search
                            .create({
                                type: 'customrecord_customer_campaign',
                                columns: ['internalid'],
                                filters: [
                                    ['custrecord_customer', search.Operator.ANYOF, customer_id],
                                    'and',
                                    [
                                        `formulanumeric: CASE WHEN {custrecord_campaign.custrecord_start_date}<=TO_DATE('${transaction_date_string}','YYYY-MM-DD') AND {custrecord_campaign.custrecord_end_date}>=TO_DATE('${transaction_date_string}','YYYY-MM-DD') THEN 1 ELSE 0 END`,
                                        search.Operator.EQUALTO,
                                        1
                                    ]
                                ]
                            })
                            .run()
                            .each(function (result) {
                                live_campaign = true;

                                return false;
                            });
                    }

                    log.debug({
                        title: 'order_within_twelve_months',
                        details: order_within_twelve_months
                    });

                    log.debug({
                        title: 'live_campaign',
                        details: live_campaign
                    });

                    for (
                        let i = 0, line_count = loaded_record.getLineCount({ sublistId: 'item' });
                        i < line_count;
                        i++
                    ) {
                        let item_classification = Number(
                            loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'custcol_so_all_rule_item_class',
                                line: i
                            })
                        );

                        log.debug({
                            title: 'item_classification',
                            details: item_classification
                        });

                        let is_le_or_lp = [
                            Number(
                                runtime
                                    .getCurrentScript()
                                    .getParameter({ name: 'custscript_sansa_so_all_le_item_class' })
                            ),
                            Number(
                                runtime
                                    .getCurrentScript()
                                    .getParameter({ name: 'custscript_sansa_so_all_lp_item_class' })
                            )
                        ].includes(item_classification);

                        if (order_within_twelve_months && !live_campaign && !is_le_or_lp) {
                            let line_id = loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'lineuniquekey',
                                line: i
                            });

                            campaigns[line_id] = {
                                item_id: loaded_record.getSublistValue({
                                    sublistId: 'item',
                                    fieldId: 'item',
                                    line: i
                                }),
                                campaign: allocation_rule_group[0].campaign
                            };
                        }
                    }

                    break;
                }
                case 'LAPSED': {
                    if (allocation_rule_group.length !== 1  || !allocation_rule_group[0].campaign) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [LAPSED] must contain exactly one Allocation Rule and specify a Campaign.'
                        });
                    }

                    let customer_id = loaded_record.getValue({ fieldId: 'entity' });
                    let transaction_date = loaded_record.getValue({ fieldId: 'trandate' });
                    let transaction_date_string =
                        transaction_date.getFullYear() +
                        '-' +
                        ('0' + (transaction_date.getMonth() + 1)).slice(-2) +
                        '-' +
                        ('0' + transaction_date.getDate()).slice(-2);

                    log.debug({
                        title: 'transaction_date_string',
                        details: transaction_date_string
                    });

                    log.debug({
                        title: 'loaded_record.id',
                        details: loaded_record.id
                    });

                    log.debug({
                        title: 'customer_id',
                        details: customer_id
                    });

                    if (typeof order_within_twelve_months === 'undefined') {
                        order_within_twelve_months = false;
                        search
                            .create({
                                type: search.Type.SALES_ORDER,
                                columns: ['internalid'],
                                filters: [
                                    ['internalidnumber', search.Operator.LESSTHAN, loaded_record.id],
                                    'and',
                                    ['customer.internalid', search.Operator.ANYOF, customer_id],
                                    'and',
                                    [
                                        `formulanumeric: CASE WHEN MONTHS_BETWEEN(TO_DATE('${transaction_date_string}','YYYY-MM-DD'),{trandate})<=12 THEN 1 ELSE 0 END`,
                                        search.Operator.EQUALTO,
                                        1
                                    ]
                                ]
                            })
                            .run()
                            .each(function (result) {
                                order_within_twelve_months = true;

                                return false;
                            });
                    }

                    if (typeof live_campaign === 'undefined') {
                        live_campaign = false;
                        search
                            .create({
                                type: 'customrecord_customer_campaign',
                                columns: ['internalid'],
                                filters: [
                                    ['custrecord_customer', search.Operator.ANYOF, customer_id],
                                    'and',
                                    [
                                        `formulanumeric: CASE WHEN {custrecord_campaign.custrecord_start_date}<=TO_DATE('${transaction_date_string}','YYYY-MM-DD') AND {custrecord_campaign.custrecord_end_date}>=TO_DATE('${transaction_date_string}','YYYY-MM-DD') THEN 1 ELSE 0 END`,
                                        search.Operator.EQUALTO,
                                        1
                                    ]
                                ]
                            })
                            .run()
                            .each(function (result) {
                                live_campaign = true;

                                return false;
                            });
                    }

                    log.debug({
                        title: 'order_within_twelve_months',
                        details: order_within_twelve_months
                    });

                    log.debug({
                        title: 'live_campaign',
                        details: live_campaign
                    });

                    if (!order_within_twelve_months && !live_campaign) {
                        campaigns['*'] = [allocation_rule_group[0].campaign];
                    }

                    break;
                }
                case 'LINE RULES': {
                    // Run through each line and try to allocate.
                    // If any line is allocated then we'll use that value for the others.
                    if (allocation_rule_group.length !== 1) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [LINE RULES] must contain exactly one Allocation Rule.'
                        });
                    }

                    let transaction_date_string = loaded_record.getText({ fieldId: 'trandate' });

                    log.debug({
                        title: 'transaction_date_string',
                        details: transaction_date_string
                    });

                    let items = [];
                    for (
                        let i = 0, line_count = loaded_record.getLineCount({ sublistId: 'item' });
                        i < line_count;
                        i++
                    ) {
                        items.push(
                            loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'item',
                                line: i
                            })
                        );
                    }

                    let item_to_most_recent_campaign = {};
                    let absolute_most_recent_allocated_campaign;
                    search
                        .create({
                            type: 'customrecord_customer_campaign',
                            columns: [
                                'custrecord_campaign.internalid',
                                'custrecord_campaign.custrecord_book_title',
                                search.createColumn({
                                    name: 'custrecord_start_date',
                                    join: 'custrecord_campaign',
                                    sort: search.Sort.DESC
                                }),
                                'custrecord_campaign.custrecord_end_date'
                            ],
                            filters: [
                                ['isinactive', search.Operator.IS, false],
                                'and',
                                [
                                    'custrecord_campaign.custrecord_start_date',
                                    search.Operator.ONORBEFORE,
                                    transaction_date_string
                                ],
                                'and',
                                [
                                    'custrecord_campaign.custrecord_end_date',
                                    search.Operator.ONORAFTER,
                                    transaction_date_string
                                ],
                                'and',
                                ['custrecord_campaign.custrecord_book_title', search.Operator.ANYOF, items],
                                'and',
                                [
                                    'custrecord_customer',
                                    search.Operator.ANYOF,
                                    loaded_record.getValue({ fieldId: 'entity' })
                                ]
                            ]
                        })
                        .run()
                        .each(function (result) {
                            let item_id_list = result.getValue({
                                name: 'custrecord_book_title',
                                join: 'custrecord_campaign'
                            });

                            let item_ids = item_id_list.split(',');
                            for (let item_id of item_ids) {
                                if (items.indexOf(item_id) < 0) {
                                    continue;
                                }

                                if (!absolute_most_recent_allocated_campaign) {
                                    absolute_most_recent_allocated_campaign = result.getValue({
                                        name: 'internalid',
                                        join: 'custrecord_campaign'
                                    });
                                }

                                if (!item_to_most_recent_campaign[item_id]) {
                                    item_to_most_recent_campaign[item_id] = result.getValue({
                                        name: 'internalid',
                                        join: 'custrecord_campaign'
                                    });
                                }
                            }

                            return true;
                        });

                    log.debug('item_to_most_recent_campaign', item_to_most_recent_campaign);
                    if (absolute_most_recent_allocated_campaign) {
                        for (let i = 0; i < items.length; i++) {
                            let line_id = loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'lineuniquekey',
                                line: i
                            });

                            campaigns[line_id] = {
                                item_id: items[i],
                                campaign:
                                    item_to_most_recent_campaign[items[i]] || absolute_most_recent_allocated_campaign
                            };
                        }
                    }

                    break;
                }
                case 'FINAL': {
                    // The backstop in case we've not allocated anything at the end.
                    if (allocation_rule_group.length !== 1 || !allocation_rule_group[0].campaign) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [FINAL] must contain exactly one Allocation Rule and specify a Campaign.'
                        });
                    }

                    campaigns['*'] = [allocation_rule_group[0].campaign];

                    break;
                }
                default: {
                    // The default case simply checks a field against a given set of values to determine whether to allocate the specified campaign.

                    // The rules may be header or line level.
                    if (!allocation_rule_group[0].is_line_level) {
                        let record_field_value = loaded_record.getValue({
                            fieldId: allocation_rule_group[0].field_id.toString().toLowerCase()
                        });

                        log.debug('record_field_value', record_field_value);
                        for (let allocation_rule of allocation_rule_group) {
                            log.debug('allocation_rule', allocation_rule);

                            for (let allocation_rule_value of allocation_rule.value) {
                                // This is deliberately "==" to cater for number/string comparison.
                                if (!campaigns['*'] && allocation_rule_value == record_field_value) {
                                    campaigns['*'] = [allocation_rule_group[i].campaign];

                                    break;
                                }
                            }
                        }
                    } else {
                        for (
                            let i = 0, line_count = loaded_record.getLineCount({ sublistId: 'item' });
                            i < line_count;
                            i++
                        ) {
                            let line_field_value = loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: allocation_rule_group[0].field_id.toString().toLowerCase(),
                                line: i
                            });

                            log.debug('line_field_value', line_field_value);
                            for (let allocation_rule of allocation_rule_group) {
                                log.debug('allocation_rule', allocation_rule);

                                let line_id = loaded_record.getSublistValue({
                                    sublistId: 'item',
                                    fieldId: 'lineuniquekey',
                                    line: i
                                });

                                for (let allocation_rule_value of allocation_rule.value) {
                                    // This is deliberately "==" to cater for number/string comparison.
                                    if (!campaigns[line_id] && allocation_rule_value == line_field_value) {
                                        campaigns[line_id] = {
                                            item_id: loaded_record.getSublistValue({
                                                sublistId: 'item',
                                                fieldId: 'item',
                                                line: i
                                            }),
                                            campaign: allocation_rule.campaign
                                        };

                                        break;
                                    }
                                }
                            }
                        }
                    }

                    break;
                }
            }

            log.audit('INFO', 'Matched Campaigns [' + JSON.stringify(campaigns) + '].');

            // The campaigns object allows for line level allocation as well as allocation to all lines (via '*').
            if (campaigns['*'] || Object.keys(campaigns).length == loaded_record.getLineCount({ sublistId: 'item' })) {
                break;
            }
        }

        return campaigns;
    }

    function cacheLoader(context) {
        if (context.key === 'allocation_rules') {
            let allocation_rules = [];

            // Load the SO Allocation Rules.
            search
                .create({
                    type: 'customrecord_sansa_so_all_rule',
                    columns: [
                        'internalid',
                        search.createColumn({ name: 'custrecord_sansa_so_all_rule_pri', sort: search.Sort.ASC }),
                        'custrecord_sansa_so_all_rule_group',
                        'custrecord_sansa_so_all_rule_group.custrecord_sansa_so_all_rule_group_fldid',
                        'custrecord_sansa_so_all_rule_value',
                        'custrecord_sansa_so_all_rule_campaign',
                        'custrecord_sansa_so_all_rule_group.custrecord_sansa_so_all_rule_line'
                    ]
                })
                .run()
                .each(function (result) {
                    let rule_group = result.getText({ name: 'custrecord_sansa_so_all_rule_group' });
                    if (
                        allocation_rules.length == 0 ||
                        allocation_rules[allocation_rules.length - 1].rule_group != rule_group
                    ) {
                        allocation_rules.push({ rule_group: rule_group, rules_list: [] });
                    }

                    allocation_rules[allocation_rules.length - 1].rules_list.push({
                        field_id: result.getValue({
                            name: 'custrecord_sansa_so_all_rule_group_fldid',
                            join: 'custrecord_sansa_so_all_rule_group'
                        }),
                        value: result.getValue({ name: 'custrecord_sansa_so_all_rule_value' }).split('\u0005'),
                        campaign: result.getValue({ name: 'custrecord_sansa_so_all_rule_campaign' }),
                        is_line_level: result.getValue({
                            name: 'custrecord_sansa_so_all_rule_line',
                            join: 'custrecord_sansa_so_all_rule_group'
                        })
                    });

                    return true;
                });

            return allocation_rules;
        }
    }

    /**
     * Marks the beginning of the Map/Reduce process and generates input data.
     *
     * @typedef {Object} ObjectRef
     * @property {number} id - Internal ID of the record instance
     * @property {string} type - Record type id
     *
     * @return {Array|Object|Search|RecordRef} inputSummary
     * @since 2015.1
     */
    function getInputData() {
        log.audit('INFO', 'START of script execution.');

        let so_allocation_cache = cache.getCache({
            name: 'so_allocation_cache',
            scope: cache.Scope.PUBLIC
        });
        so_allocation_cache.remove({ key: 'allocation_rules' });

        // Search for anything that is missing an allocation.
        return search.create({
            type: record.Type.SALES_ORDER,
            columns: ['internalid'],
            filters: [
                ['mainline', search.Operator.IS, true],
                'and',
                ['custbody_sansa_so_allocation_complete', search.Operator.IS, false],
                'and',
                ['trandate', search.Operator.ONORAFTER, '6/6/2020']
            ]
        });
    }

    /**
     * Executes when the reduce entry point is triggered and applies to each group.
     *
     * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
     * @since 2015.1
     */
    function reduce(context) {
        log.debug('context', context);

        try {
            let sales_order = record.load({ type: record.Type.SALES_ORDER, id: context.key });

            let campaigns = getCampaigns(sales_order);

            for (let i = 0, line_count = sales_order.getLineCount({ sublistId: 'item' }); i < line_count; i++) {
                // Using try/catch to set any lines where we possibly can.
                try {
                    let campaign;
                    let line_id = sales_order.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'lineuniquekey',
                        line: i
                    });

                    if (campaigns[line_id]) {
                        campaign = campaigns[line_id].campaign;
                        log.debug('campaigns[line_id]', campaigns[line_id]);

                        let item_id = sales_order.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'item',
                            line: i
                        });

                        // This really shouldn't happen.
                        if (!campaign || item_id != campaigns[line_id].item_id) {
                            throw error.create({ name: 'UNEXPECTED_ERROR', message: 'Unexpected Error' });
                        }
                    } else {
                        campaign = campaigns['*'];
                    }

                    log.audit('INFO', 'Updating SO Line [' + i + '] with Campaign [' + Number(campaign) + '].');

                    sales_order.setSublistValue({
                        sublistId: 'item',
                        fieldId: 'class',
                        value: Number(campaign),
                        line: i
                    });
                } catch (err) {
                    log.error('LINE_ERROR', err);
                }
            }

            sales_order.save();
        } catch (err) {
            log.error('RECORD_ERROR', err);
        }

        // We must update the status to make way for auto-invoicing.
        log.audit('INFO', 'Updating the status of the Sales Order.');
        record.submitFields({
            type: record.Type.SALES_ORDER,
            id: context.key,
            values: {
                custbody_sansa_so_allocation_complete: true
            }
        });
    }

    /**
     * Executes when the summarize entry point is triggered and applies to the result set.
     *
     * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
     * @since 2015.1
     */
    function summarize(summaryContext) {
        // INPUT Error
        let input_error_string = '';
        if (summaryContext.inputSummary.error) {
            input_error_string += `GET INPUT DATA ERRORS\n\n`;
            input_error_string += `[Details: ${summaryContext.inputSummary.error}]\n\n`;
        }
        if (input_error_string.length > 0) {
            log.error({
                title: 'INPUT_ERROR',
                details: input_error_string
            });
        }

        // MAP Errors
        let map_error_string = '';
        summaryContext.mapSummary.errors.iterator().each(function (key, err) {
            if (map_error_string.length === 0) {
                map_error_string = `MAP ERRORS\n\n`;
            }
            map_error_string += `[MAP    Error for Key: ${key} Details: ${err}]\n\n`;
            return true;
        });
        if (map_error_string.length > 0) {
            log.error({
                title: 'MAP_ERRORS',
                details: map_error_string
            });
        }

        // REDUCE Errors
        let reduce_error_string = '';
        summaryContext.reduceSummary.errors.iterator().each(function (key, err) {
            if (reduce_error_string.length === 0) {
                reduce_error_string = `REDUCE ERRORS\n\n`;
            }
            reduce_error_string += `[REDUCE Error for Key: ${key} Details: ${err}]\n\n`;
            return true;
        });
        if (reduce_error_string.length > 0) {
            log.error({
                title: 'REDUCE_ERRORS',
                details: reduce_error_string
            });
        }

        log.audit('INFO', 'END of script execution.');

        // Raise the errors.
        if (input_error_string.length > 0 || map_error_string.length > 0 || reduce_error_string.length > 0) {
            throw error.create({
                name: 'MAP_REDUCE_ERROR',
                message: `${input_error_string}${map_error_string}${reduce_error_string}`
            });
        }
    }

    return {
        getInputData: getInputData,
        reduce: reduce,
        summarize: summarize
    };
});
