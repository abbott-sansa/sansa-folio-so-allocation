/**
 * Sales Order Allocation
 * Customises the UI to streamline the management of SO Allocation Rules.
 *
 * Version      Date                Author                      Remarks
 * 1.0          04 Mar 2020         Chris Abbott                N/A
 *
 * @NApiVersion 2.x
 * @NScriptType ClientScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/search'],

function(record, search) {

    var dynamic_fields = [];

    /**
     * Function to be executed after page is initialized.
     *
     * @param {Object} scriptContext
     * @param {Record} scriptContext.currentRecord - Current form record
     * @param {string} scriptContext.mode - The mode in which the record is being accessed (create, copy, or edit)
     *
     * @since 2015.2
     */
    function pageInit(scriptContext) {
        // Set up the form and fields based on the currently selected Rule Group, if any.
        var rule_group = scriptContext.currentRecord.getValue({fieldId: 'custrecord_sansa_so_all_rule_group'});

        search.create({
            type: 'customrecord_sansa_so_all_rule_group',
            columns: ['internalid', 'name', 'custrecord_sansa_so_all_rule_group_fld_l'],
            filters: ['custrecord_sansa_so_all_rule_group_fld', search.Operator.NONEOF, '@NONE@']
        }).run().each(function (result) {
            var field_id = 'custpage_sansa_select_' + result.getValue({name: 'internalid'})

            // Maintain the list of dynamic fields for use later.
            dynamic_fields.push(field_id);

            // Hide everything but the currently selected Rule Group and set its value, if provided.
            if (!field_id.endsWith('_' + rule_group)) {
                scriptContext.currentRecord.getField({fieldId: field_id}).isDisplay = false;
            } else {
                var current_values = scriptContext.currentRecord.getValue({fieldId: 'custrecord_sansa_so_all_rule_value'});
                scriptContext.currentRecord.setValue({fieldId: field_id, value: current_values})
            }

            return true;
        });
    }

    /**
     * Function to be executed when field is changed.
     *
     * @param {Object} scriptContext
     * @param {Record} scriptContext.currentRecord - Current form record
     * @param {string} scriptContext.sublistId - Sublist name
     * @param {string} scriptContext.fieldId - Field name
     * @param {number} scriptContext.lineNum - Line number. Will be undefined if not a sublist or matrix field
     * @param {number} scriptContext.columnNum - Line number. Will be undefined if not a matrix field
     *
     * @since 2015.2
     */
    function fieldChanged(scriptContext) {
        // Handle changes to the Rule Group.
        if (scriptContext.fieldId == 'custrecord_sansa_so_all_rule_group') {
            scriptContext.currentRecord.setValue({fieldId: 'custrecord_sansa_so_all_rule_value', value: null});
            scriptContext.currentRecord.setText({fieldId: 'custrecord_sansa_so_all_rule_text', text: null});
            scriptContext.currentRecord.setText({fieldId: 'custrecord_sansa_so_all_rule_campaign', text: null});

            var rule_group = scriptContext.currentRecord.getValue({fieldId: 'custrecord_sansa_so_all_rule_group'});
            for (var i = 0; i < dynamic_fields.length; i++) {
                var field_id = dynamic_fields[i];

                // As per pageInit, hide everything but the currently selected Rule Group and set its value, if provided.
                if (field_id.endsWith('_' + rule_group)) {
                    scriptContext.currentRecord.getField({fieldId: field_id}).isDisplay = true;
                } else {
                    scriptContext.currentRecord.getField({fieldId: field_id}).isDisplay = false;
                }
            }
        }

        // Handle changes to the dynamic Rule Group fields and put the values in the stored fields.
        if (scriptContext.fieldId.startsWith('custpage_sansa_select_')) {
            if (scriptContext.currentRecord.getField({fieldId: scriptContext.fieldId}).type == 'select') {
                scriptContext.currentRecord.setValue({
                    fieldId: 'custrecord_sansa_so_all_rule_value',
                    value: scriptContext.currentRecord.getValue({fieldId: scriptContext.fieldId}).join(',')
                });
                scriptContext.currentRecord.setText({
                    fieldId: 'custrecord_sansa_so_all_rule_text',
                    text: scriptContext.currentRecord.getText({fieldId: scriptContext.fieldId}).join(',')
                });
            } else {
                scriptContext.currentRecord.setValue({
                    fieldId: 'custrecord_sansa_so_all_rule_value',
                    value: scriptContext.currentRecord.getValue({fieldId: scriptContext.fieldId})
                });
                scriptContext.currentRecord.setText({
                    fieldId: 'custrecord_sansa_so_all_rule_text',
                    text: scriptContext.currentRecord.getText({fieldId: scriptContext.fieldId})
                });
            }
        }
    }

    return {
        pageInit: pageInit,
        fieldChanged: fieldChanged
    };
    
});
