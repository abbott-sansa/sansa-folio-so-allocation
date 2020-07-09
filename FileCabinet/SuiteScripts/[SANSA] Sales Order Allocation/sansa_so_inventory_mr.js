/**
 * Sales Order Inventory
 * Automates Inventory Detail and Analysis Code on Sales Orders.
 *
 * Version      Date                Author                      Remarks
 * 1.0          01 Jul 2020         Chris Abbott                Moved Lot Number and Analysis Code automation to a separate script.
 *
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/email', 'N/error', 'N/file', 'N/record', 'N/render', 'N/runtime', 'N/search'],

    function (email, error, file, record, render, runtime, search) {

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

            const process_all_orders = runtime.getCurrentScript().getParameter({name: 'custscript_sansa_so_inventory_all_orders'});

            var filters = [
                ['mainline', search.Operator.IS, false],
                'and',
                ['item.islotitem', search.Operator.IS, true],
                'and',
                ['inventorydetail.internalid', search.Operator.ANYOF, '@NONE@'],
                'and',
                ['trandate', search.Operator.ONORAFTER, '6/6/2020'],
            ];




            const MAX_HOURS_OLD = 3;
            if (!process_all_orders) {
                // Update the Search to only include orders created within the last 3 hours.
                log.audit('INFO', 'The current script deployment is configured to process the most recent orders.');
                filters = filters.concat(['and', ['datecreated', search.Operator.ON, 'today'], 'and', ['formulanumeric: (TO_NUMBER(TO_CHAR({today}, \'HH24\'))+TO_NUMBER(TO_CHAR({today}, \'MI\'))/60)-(TO_NUMBER(TO_CHAR({datecreated}, \'HH24\'))+TO_NUMBER(TO_CHAR({datecreated}, \'MI\'))/60)', search.Operator.LESSTHANOREQUALTO, MAX_HOURS_OLD]]);
            } else {
                log.audit('INFO', 'The current script deployment is configured to process all outstanding orders.');
            }


            log.debug('filters', filters);

            // Search for anything that is missing Inventory Detail.
            return search.create({
                type: record.Type.SALES_ORDER,
                columns: ['internalid'],
                filters: filters
            });
        }

        /**
         * Executes when the map entry point is triggered and applies to each key/value pair.
         *
         * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
         * @since 2015.1
         */
        function map(context) {

        }

        /**
         * Executes when the reduce entry point is triggered and applies to each group.
         *
         * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
         * @since 2015.1
         */
        function reduce(context) {
            log.debug('context', context);

            var sales_order = record.load({type: record.Type.SALES_ORDER, id: context.key});

            var items = [];
            for (var i = 0, line_count = sales_order.getLineCount({sublistId: 'item'}); i < line_count; i++) {
                var existing_inventory_detail = sales_order.hasSublistSubrecord({
                    sublistId: 'item',
                    fieldId: 'inventorydetail',
                    line: i
                });
                
                if (!existing_inventory_detail) {
                    items.push(sales_order.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'item',
                        line: i
                    }));
                }
            }

            log.debug('items', items);

            // TODO - Is it possible to deal with an errored line?
            // TODO - e.g. https://5602569-sb1.app.netsuite.com/app/accounting/transactions/salesord.nl?id=83331



            if (!sales_order.getValue({fieldId: 'location'})) {
                log.audit('INFO', 'ABORTING - Cannot assign Inventory Detail to a Sales Order without Location.');
                return;
            }

            
            var item_inventory_numbers = {};
            var analysis_code_filter = [];
            search.create({
                type: 'inventorynumber',
                columns: ['internalid', 'item', 'inventorynumber', 'quantityavailable'],
                filters: [
                    ['item', search.Operator.ANYOF, items],
                    'and',
                    ['location', search.Operator.ANYOF, sales_order.getValue({fieldId: 'location'})],
                    'and',
                    ['quantityavailable', search.Operator.GREATERTHAN, 0]
                ]
            }).run().each(function (result) {
                if (!item_inventory_numbers[result.getValue({name: 'item'})]) {
                    item_inventory_numbers[result.getValue({name: 'item'})] = {
                        inventory_number_id: result.getValue({name: 'internalid'}),
                        inventory_number_text: result.getValue({name: 'inventorynumber'}),
                        available_quantity: result.getValue({name: 'quantityavailable'})
                    };

                    if (analysis_code_filter.length > 0) {
                        analysis_code_filter.push('or');
                    }
                    analysis_code_filter.push(['name', search.Operator.IS, result.getValue({name: 'inventorynumber'})]);
                }

                return true;
            });

            log.debug('item_inventory_numbers', item_inventory_numbers);



            if (Object.keys(item_inventory_numbers).length == 0) {
                log.audit('INFO', 'ABORTING - There is no stock for any Item on the Sales Order.');
                return;
            }

            log.debug('analysis_code_filter', analysis_code_filter);


            var valid_analysis_codes = [];
            search.create({
                type: 'customrecord_csegcseg_anal_segme',
                columns: ['name'],
                filters: analysis_code_filter
            }).run().each(function (result) {
                log.debug('result', result);
                valid_analysis_codes.push(result.getValue({name: 'name'}));
                
                return false;
            });

            log.debug('valid_analysis_codes', valid_analysis_codes);


            // Try to automatically assign an Inventory Number and set the Analysis Code using ther above information.
            for (var i = 0, line_count = sales_order.getLineCount({sublistId: 'item'}); i < line_count; i++) {
                try {
                    var item_inventory_number_text;
                    var existing_inventory_detail = sales_order.hasSublistSubrecord({
                        sublistId: 'item',
                        fieldId: 'inventorydetail',
                        line: i
                    });
                    var inventory_detail = sales_order.getSublistSubrecord({
                        sublistId: 'item',
                        fieldId: 'inventorydetail',
                        line: 0
                    });

                    if (existing_inventory_detail) {
                        if (!sales_order.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'csegcseg_anal_segme',
                            line: i
                        })) {
                            item_inventory_number_text = inventory_detail.getSublistText({
                                sublistId: 'inventoryassignment',
                                fieldId: 'issueinventorynumber',
                                line: 0
                            });
                        }
                    } else {
                        var item_quantity = sales_order.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'quantity',
                            line: i
                        });

                        var item_inventory_number = item_inventory_numbers[sales_order.getSublistValue({
                            sublistId: 'item',
                            fieldId: 'item',
                            line: i
                        })];

                        if (!item_inventory_number) {
                            continue;
                        }

                        var item_inventory_number_id = item_inventory_number.inventory_number_id;
                        item_inventory_number_text = item_inventory_number.inventory_number_text;

                        var inventory_detail = sales_order.getSublistSubrecord({
                            sublistId: 'item',
                            fieldId: 'inventorydetail',
                            line: i
                        });

                        inventory_detail.setSublistValue({
                            sublistId: 'inventoryassignment',
                            fieldId: 'issueinventorynumber',
                            value: item_inventory_number_id,
                            line: 0
                        });

                        inventory_detail.setSublistValue({
                            sublistId: 'inventoryassignment',
                            fieldId: 'quantity',
                            value: Math.min(item_quantity, item_inventory_number.available_quantity),
                            line: 0
                        });
                    }

                    if (item_inventory_number_text) {
                        if (valid_analysis_codes.indexOf(item_inventory_number_text) >= 0) {
                            sales_order.setSublistText({
                                sublistId: 'item',
                                fieldId: 'csegcseg_anal_segme',
                                text: item_inventory_number_text,
                                line: i
                            });
                        } else {
                            log.error('ERROR', 'The Inventory Number specified on the Inventory Detail record is not a valid Analysis Code.')
                        }
                    }
                } catch(err) {
                    log.error('ERROR', err);
                }
            }

            sales_order.save();
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