﻿/// <reference path="../../Scripts/typings/jquery.dynatree/jquery.dynatree.d.ts" />

import composition = require("durandal/composition");
import appUrl = require("common/appUrl");
import getFoldersCommand = require("commands/filesystem/getFoldersCommand");

/*
 * A custom Knockout binding handler transforms the target element (a <div>) into a tree, powered by jquery-dynatree
 * Usage: data-bind="tree: { value: someObservableTreeObject }"
 */
class treeBindingHandler {

    static install() {
        if (!ko.bindingHandlers["tree"]) {
            ko.bindingHandlers["tree"] = new treeBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("tree");
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var options: {
            selectedNode: KnockoutObservable<string>;
            addedNode: KnockoutObservable<string>;
            currentLevelNodes: KnockoutObservableArray<string>;
        } = <any>ko.utils.unwrapObservable(valueAccessor());

        var tree = $(element).dynatree({
            children: [{ title: appUrl.getFilesystem().name, key: "#", isLazy: true, isFolder: true }],
            onLazyRead: function (node) {
                var dir;
                if (node.data && node.data.key) {
                    dir = node.data.key;
                }
                var command = new getFoldersCommand(appUrl.getFilesystem(), 0, 100, dir);
                command.execute().done((results: folderNodeDto[]) => {
                    node.setLazyNodeStatus(0);
                    node.addChild(results);
                    options.currentLevelNodes(results.map(x => x.key));
                });
            },
            selectMode: 1,
            onSelect: function (flag, node) {
                treeBindingHandler.onActivateAndSelect(node, valueAccessor());
            },
            onActivate: function (node) {
                treeBindingHandler.onActivateAndSelect(node, valueAccessor());
            }
        });

        var firstNode = (<DynaTreeNode>$(element).dynatree("getRoot", [])).getChildren()[0];
        firstNode.activate();
        firstNode.expand(null);
    }

    static onActivateAndSelect(node, valueAccessor: any) {
        var options: {
            selectedNode: KnockoutObservable<string>;
            addedNode: KnockoutObservable<string>;
            currentLevelNodes: KnockoutObservableArray<string>;
        } = <any>ko.utils.unwrapObservable(valueAccessor);

        var selectedNode = node.data && node.data.key != "#" ? node.data.key : null;
        options.selectedNode(selectedNode);
        if (node.data) {
            var siblings = [];
            if (node.hasChildren()) {
               siblings = node.getChildren();
            }
            var mappedNodes = siblings.map(x => x.data.title);
            options.currentLevelNodes(mappedNodes);
        }
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var options: {
            selectedNode: KnockoutObservable<string>;
            addedNode: KnockoutObservable<folderNodeDto>;
            currentLevelNodes: KnockoutObservableArray<string>;
        } = <any>ko.utils.unwrapObservable(valueAccessor());
        if (options.addedNode()) {
            var activeNode = <DynaTreeNode>$(element).dynatree("getActiveNode", []);
            if (activeNode) {
                activeNode.addChild(options.addedNode());
                options.currentLevelNodes(activeNode.getChildren().map(x => x.data.title));
                options.addedNode(null);
            }
        }
    }

    static updateNodeHierarchyStyle(tree: string, key: string, styleClass?: string) {
        var dynaTree = $(tree).dynatree("getTree");
        var slashPosition = key.length;
        while (slashPosition > 0) {
            key = key.substring(0, slashPosition);
            var temporaryNode = dynaTree.getNodeByKey(key);
            if (temporaryNode.data.addClass != styleClass) {
                temporaryNode.data.addClass = styleClass
                temporaryNode.reloadChildren();
            }

            slashPosition = key.lastIndexOf("/");
        }
    }
}

export = treeBindingHandler;