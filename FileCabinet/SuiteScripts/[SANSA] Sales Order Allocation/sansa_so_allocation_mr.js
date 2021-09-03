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
 *
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/email', 'N/error', 'N/file', 'N/record', 'N/render', 'N/runtime', 'N/search', 'N/format'],

    function (email, error, file, record, render, runtime, search, format) {

        var order_within_twelve_months;
        var live_campaign;

        // Function used to determine the Campaign associated with a particular Rule Group.
        function getCampaigns(campaigns, options) {
            if (options.allocation_rule_group === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::allocation_rule_group is a required argument.'
                });
            }
            if (options.allocation_rule_group_name === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::allocation_rule_group_name is a required argument.'
                });
            }
            if (options.loaded_record === undefined) {
                throw error.create({
                    name: 'MISSING_REQUIRED_ARGUMENT',
                    message: 'getCampaign:::loaded_record is a required argument.'
                });
            }

            log.audit('INFO', 'Evaluating Rule Group [' + options.allocation_rule_group_name + '].')
            switch (options.allocation_rule_group_name) {
                // Apart from the default case, all others implement custom logic.
                case 'VOUCHER CODES':
                    // Check whether the Voucher Code is associated with a Campaign.
                    if (options.allocation_rule_group.length != 1 || !options.allocation_rule_group[0].field_id) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [VOUCHER CODES] must contain exactly one Allocation Rule and specify a field ID.'
                        });
                    }

                    var voucher_code = options.loaded_record.getValue({fieldId: options.allocation_rule_group[0].field_id.toString().toLowerCase()});
                    if (!voucher_code) {
                        break;
                    }

                    var campaign_value;
                    search.create({
                        type: 'customrecordfsl_vouchercode',
                        columns: ['custrecordfsl_campaign'],
                        filters: [
                            ['isinactive', search.Operator.IS, false],
                            'and',
                            ['custrecord118', search.Operator.IS, voucher_code]
                        ]
                    }).run().each(function (result) {
                        if (campaign_value) {
                            throw error.create({
                                name: 'VOUCHER_CODE_NOT_UNIQUE',
                                message: 'Unable to uniquely identify a voucher code as multiple were found.'
                            });
                        }
                        campaign_value = [result.getValue({name: 'custrecordfsl_campaign'})];

                        return true;
                    });

                    if (campaign_value) {
                        campaigns['*'] = campaign_value;
                    }

                    break;
                case 'CURRENT':
                    if (options.allocation_rule_group.length != 1) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [CURRENT] must contain exactly one Allocation Rule.'
                        });
                    }

                    var customer_id = options.loaded_record.getValue({fieldId: 'entity'});
                    var transaction_date = options.loaded_record.getValue({fieldId: 'trandate'});
                    var transaction_date_string = transaction_date.getFullYear() + '-' + ('0' + (transaction_date.getMonth()+1)).slice(-2) + '-' + ('0' + transaction_date.getDate()).slice(-2);

                    if (typeof order_within_twelve_months === 'undefined') {
                        order_within_twelve_months = false
                        search.create({
                            type: search.Type.SALES_ORDER,
                            columns: ['internalid'],
                            filters: [
                                ['internalidnumber', search.Operator.LESSTHAN, options.loaded_record.id],
                                'and',
                                ['customer.internalid', search.Operator.ANYOF, customer_id],
                                'and',
                                ['formulanumeric: CASE WHEN MONTHS_BETWEEN(DATE \'' + transaction_date_string + '\',{trandate})<=12 THEN 1 ELSE 0 END', search.Operator.EQUALTO, 1]
                            ]
                        }).run().each(function (result) {
                            order_within_twelve_months = true;

                            return false;
                        });
                    }

                    if (typeof live_campaign === 'undefined') {
                        live_campaign = false
                        search.create({
                            type: 'customrecord_customer_campaign',
                            columns: ['internalid'],
                            filters: [
                                ['custrecord_customer', search.Operator.ANYOF, customer_id],
                                'and',
                                ['formulanumeric: CASE WHEN {custrecord_campaign.custrecord_start_date}<=DATE \'' + transaction_date_string + '\' AND {custrecord_campaign.custrecord_end_date}>=DATE \'' + transaction_date_string + '\' THEN 1 ELSE 0 END', search.Operator.EQUALTO, 1]
                            ]
                        }).run().each(function (result) {
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

                    if (order_within_twelve_months && !live_campaign) {
                        campaigns['*'] = [options.allocation_rule_group[0].campaign];
                    }

                    break;
                case 'LAPSED':
                    if (options.allocation_rule_group.length != 1) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [LAPSED] must contain exactly one Allocation Rule.'
                        });
                    }

                    var customer_id = options.loaded_record.getValue({fieldId: 'entity'});
                    var transaction_date = options.loaded_record.getValue({fieldId: 'trandate'});
                    var transaction_date_string = transaction_date.getFullYear() + '-' + ('0' + (transaction_date.getMonth()+1)).slice(-2) + '-' + ('0' + transaction_date.getDate()).slice(-2);

                    if (typeof order_within_twelve_months === 'undefined') {
                        order_within_twelve_months = false
                        search.create({
                            type: search.Type.SALES_ORDER,
                            columns: ['internalid'],
                            filters: [
                                ['internalidnumber', search.Operator.LESSTHAN, options.loaded_record.id],
                                'and',
                                ['customer.internalid', search.Operator.ANYOF, customer_id],
                                'and',
                                ['formulanumeric: CASE WHEN MONTHS_BETWEEN(DATE \'' + transaction_date_string + '\',{trandate})<=12 THEN 1 ELSE 0 END', search.Operator.EQUALTO, 1]
                            ]
                        }).run().each(function (result) {
                            order_within_twelve_months = true;

                            return false;
                        });
                    }

                    if (typeof live_campaign === 'undefined') {
                        live_campaign = false
                        search.create({
                            type: 'customrecord_customer_campaign',
                            columns: ['internalid'],
                            filters: [
                                ['custrecord_customer', search.Operator.ANYOF, customer_id],
                                'and',
                                ['formulanumeric: CASE WHEN {custrecord_campaign.custrecord_start_date}<=DATE \'' + transaction_date_string + '\' AND {custrecord_campaign.custrecord_end_date}>=DATE \'' + transaction_date_string + '\' THEN 1 ELSE 0 END', search.Operator.EQUALTO, 1]
                            ]
                        }).run().each(function (result) {
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
                        campaigns['*'] = [options.allocation_rule_group[0].campaign];
                    }

                    break;
                case 'LINE RULES':
                    // Run through each line and try to allocate.
                    // If any line is allocated then we'll use that value for the others.
                    if (options.allocation_rule_group.length != 1) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [LINE RULES] must contain exactly one Allocation Rule.'
                        });
                    }

                    var items = [];
                    for (var i = 0, line_count = options.loaded_record.getLineCount({sublistId: 'item'}); i < line_count; i++) {
                        items.push(options.loaded_record.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'item',
                            line: i
                        }));
                    }

                    var item_to_most_recent_campaign = {};
                    var absolute_most_recent_allocated_campaign;
                    search.create({
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
                            ['custrecord_campaign.custrecord_start_date', search.Operator.ONORBEFORE, 'today'],
                            'and',
                            ['custrecord_campaign.custrecord_end_date', search.Operator.ONORAFTER, 'today'],
                            'and',
                            ['custrecord_campaign.custrecord_book_title', search.Operator.ANYOF, items],
                            'and',
                            ['custrecord_customer', search.Operator.ANYOF, options.loaded_record.getValue({fieldId: 'entity'})]
                        ],
                    }).run().each(function (result) {
                        var item_id_list = result.getValue({
                            name: 'custrecord_book_title',
                            join: 'custrecord_campaign'
                        });

                        var item_ids = item_id_list.split(',');
                        for (var i in item_ids) {
                            var item_id = item_ids[i];

                            if (items.indexOf(item_id) < 0) {
                                continue;
                            }

                            if (!absolute_most_recent_allocated_campaign) {
                                absolute_most_recent_allocated_campaign = result.getValue({
                                    name: 'internalid',
                                    join: 'custrecord_campaign'
                                })
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
                        for (var i = 0; i < items.length; i++) {
                            campaigns[options.loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'lineuniquekey',
                                line: i
                            })] = {
                                item_id: items[i],
                                campaign: item_to_most_recent_campaign[items[i]] || absolute_most_recent_allocated_campaign
                            };
                        }
                    }

                    break;
                case 'FINAL':
                    // The backstop in case we've not allocated anything at the end.
                    if (options.allocation_rule_group.length != 1 || !options.allocation_rule_group[0].campaign) {
                        throw error.create({
                            name: 'INVALID_ALLOCATION_RULE_GROUP',
                            message: 'Group [FINAL] must contain exactly one Allocation Rule and specify a Campaign.'
                        });
                    }

                    campaigns['*'] = [options.allocation_rule_group[0].campaign];

                    break;
                default:
                    // The default case simply checks a field against a given set of values to determine whether to allocate the specified campaign.

                    // The rules may be header or line level.
                    if (!options.allocation_rule_group[0].is_line_level) {
                        var record_field_value = options.loaded_record.getValue({fieldId: options.allocation_rule_group[0].field_id.toString().toLowerCase()});
                        if (!record_field_value) {
                            record_field_value = '';
                        }

                        log.debug('record_field_value', record_field_value);
                        for (var i = 0; i < options.allocation_rule_group.length; i++) {
                            log.debug('options.allocation_rule_group[i]', options.allocation_rule_group[i]);

                            if (options.allocation_rule_group[i].value.indexOf(record_field_value) >= 0) {
                                campaigns['*'] = [options.allocation_rule_group[i].campaign];

                                break;
                            }
                        }
                    } else {
                        for (var i = 0, line_count = options.loaded_record.getLineCount({sublistId: 'item'}); i < line_count; i++) {
                            var line_field_value = options.loaded_record.getSublistValue({
                                sublistId: 'item',
                                fieldId: options.allocation_rule_group[0].field_id.toString().toLowerCase(),
                                line: i
                            });
                            if (!line_field_value) {
                                line_field_value = '';
                            }

                            log.debug('line_field_value', line_field_value);
                            for (var j = 0; j < options.allocation_rule_group.length; j++) {
                                log.debug('options.allocation_rule_group[j]', options.allocation_rule_group[j]);

                                var line_id = options.loaded_record.getSublistValue({
                                    sublistId: 'item',
                                    fieldId: 'lineuniquekey',
                                    line: i
                                });

                                if (!campaigns[line_id] && options.allocation_rule_group[j].value.indexOf(line_field_value) >= 0) {
                                    campaigns[line_id] = {
                                        item_id: options.loaded_record.getSublistValue({
                                            sublistId: 'item',
                                            fieldId: 'item',
                                            line: i
                                        }),
                                        campaign: options.allocation_rule_group[j].campaign
                                    };

                                    break;
                                }
                            }
                        }
                    }

                    break;
            }

            log.audit('INFO', 'Matched Campaigns [' + JSON.stringify(campaigns) + '].')
            return campaigns;
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
                // Load the SO Allocation Rules.
                var allocation_rules = [];
                search.create({
                    type: 'customrecord_sansa_so_all_rule',
                    columns: [
                        'internalid',
                        search.createColumn({name: 'custrecord_sansa_so_all_rule_pri', sort: search.Sort.ASC}),
                        'custrecord_sansa_so_all_rule_group',
                        'custrecord_sansa_so_all_rule_group.custrecord_sansa_so_all_rule_group_fldid',
                        'custrecord_sansa_so_all_rule_value',
                        'custrecord_sansa_so_all_rule_campaign',
                        'custrecord_sansa_so_all_rule_group.custrecord_sansa_so_all_rule_line'
                    ]
                }).run().each(function (result) {
                    var rule_group = result.getText({name: 'custrecord_sansa_so_all_rule_group'});
                    if (allocation_rules.length == 0 || allocation_rules[allocation_rules.length - 1].rule_group != rule_group) {
                        allocation_rules.push({rule_group: rule_group, rules_list: []});
                    }

                    allocation_rules[allocation_rules.length - 1].rules_list.push({
                        field_id: result.getValue({
                            name: 'custrecord_sansa_so_all_rule_group_fldid',
                            join: 'custrecord_sansa_so_all_rule_group'
                        }),
                        value: result.getValue({name: 'custrecord_sansa_so_all_rule_value'}).split('\u0005'),
                        campaign: result.getValue({name: 'custrecord_sansa_so_all_rule_campaign'}),
                        is_line_level: result.getValue({
                            name: 'custrecord_sansa_so_all_rule_line',
                            join: 'custrecord_sansa_so_all_rule_group'
                        })
                    });

                    return true;
                });

                log.debug('allocation_rules', allocation_rules);

                var sales_order = record.load({type: record.Type.SALES_ORDER, id: context.key});
                var campaigns = {};

                for (var i=0; i<allocation_rules.length; i++) {
                    log.debug(allocation_rules[i].rule_group, allocation_rules[i].rules_list);

                    // Using try/catch after the decision to continue down the list in the event of any error.
                    try {
                        campaigns = getCampaigns(campaigns, {
                            allocation_rule_group_name: allocation_rules[i].rule_group,
                            allocation_rule_group: allocation_rules[i].rules_list,
                            loaded_record: sales_order
                        });
                    } catch (err) {
                        log.error('ERROR1', err);
                    }

                    // The campaigns object allows for line level allocation as well as a default value provided by '*'.
                    log.debug('campaigns', campaigns);
                    if (campaigns['*'] || Object.keys(campaigns).length == sales_order.getLineCount({sublistId: 'item'})) {
                        break;
                    }
                }

                for (var i = 0, line_count = sales_order.getLineCount({sublistId: 'item'}); i < line_count; i++) {
                    // Using try/catch to set any lines where we possibly can.
                    try {
                        var campaign;
                        var line_id = sales_order.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'lineuniquekey',
                            line: i
                        });

                        if (campaigns[line_id]) {
                            campaign = campaigns[line_id].campaign;
                            log.debug('campaigns[line_id]', campaigns[line_id]);

                            var item_id = sales_order.getSublistValue({
                                sublistId: 'item',
                                fieldId: 'item',
                                line: i
                            });

                            // This really shouldn't happen.
                            if (!campaign || item_id != campaigns[line_id].item_id) {
                                throw error.create({name: 'UNEXPECTED_ERROR', message: 'Unexpected Error'});
                            }
                        } else {
                            campaign = campaigns['*'];
                        }

                        log.audit('INFO', 'Updating SO Line [' + i + '] with Campaign [' + Number(campaign) + '].')

                        sales_order.setSublistValue({
                            sublistId: 'item',
                            fieldId: 'class',
                            value: Number(campaign),
                            line: i
                        });
                    } catch (err) {
                        log.error('ERROR2', err);
                    }
                }

                sales_order.save();
            } catch (err) {
                log.error('ERROR3', err);
            }

            // We must update the status to make way for auto-invoicing.
            log.audit('INFO', 'Updating the status of the Sales Order.')
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
        function summarize(summary) {
            function getScriptOwner() {
                // Default to the account creator.
                var script_owner = -5;

                // Search for the current script record based on runtime information.
                try {
                    search.create({
                        type: 'script',
                        columns: 'owner',
                        filters: ['scriptid', search.Operator.IS, runtime.getCurrentScript().id]
                    }).run().each(function (result) {
                        script_owner = result.getValue({name: 'owner'});
                    });
                } catch (err) {
                }

                return script_owner;
            }

            // Function used in handling errors for logging and email notifications.
            function doNotify(stage, error_string) {
                log.error('Map/Reduce Script [' + runtime.getCurrentScript().id + '] Error(s) at Stage: ' + stage, error_string);
                email.send({
                    author: getScriptOwner(),
                    recipients: getScriptOwner(),
                    subject: 'Map/Reduce Script [' + runtime.getCurrentScript().id + '] Error(s) at Stage: ' + stage,
                    body: 'Error(s) occurred, as detailed below:\n' + error_string
                });
            }

            // Handle INPUT errors.
            if (summary.inputSummary.error) {
                doNotify('getInputData', summary.inputSummary.error);
                throw summary.inputSummary.error;
            }

            // Handle MAP errors.
            var map_error_string = '';
            summary.mapSummary.errors.iterator().each(function (key, err) {
                map_error_string += '[Error for Key: ' + key + '\n Details:\n' + err + ']\n\n';
                return true;
            });

            if (map_error_string.length > 0) {
                doNotify('map', map_error_string);
            }

            // Handle REDUCE errors.
            var reduce_error_string = '';
            summary.reduceSummary.errors.iterator().each(function (key, err) {
                reduce_error_string += '[Error for Key: ' + key + '\n Details:\n' + err + ']\n\n';
                return true;
            });

            if (reduce_error_string.length > 0) {
                doNotify('reduce', reduce_error_string);
            }

            log.audit('INFO', 'END of script execution.');
        }

        return {
            getInputData: getInputData,
            reduce: reduce,
            summarize: summarize
        };

    });